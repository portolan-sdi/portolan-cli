"""Replay two Philadelphia ArcGIS services for documentation tests."""

from __future__ import annotations

import argparse
import json
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import mapping
from shapely.wkb import loads as load_wkb


@dataclass(frozen=True)
class Layer:
    """One replayed ArcGIS FeatureServer layer."""

    service_name: str
    layer_name: str
    geometry_type: str
    description: str
    features: list[dict[str, Any]]
    fields: list[dict[str, Any]]


def _arcgis_field(field: pa.Field) -> dict[str, Any]:
    """Map one Arrow field to the small ArcGIS schema subset under test."""
    if field.name == "objectid":
        field_type = "esriFieldTypeOID"
    elif pa.types.is_integer(field.type):
        field_type = "esriFieldTypeInteger"
    elif pa.types.is_floating(field.type):
        field_type = "esriFieldTypeDouble"
    else:
        field_type = "esriFieldTypeString"
    return {
        "name": field.name,
        "type": field_type,
        "alias": field.name.replace("_", " ").title(),
    }


def _load_layer(
    fixture: Path,
    *,
    service_name: str,
    layer_name: str,
    geometry_type: str,
    description: str,
) -> Layer:
    """Convert a pinned GeoParquet Asset to ArcGIS GeoJSON responses."""
    table = pq.read_table(fixture)
    fields = [
        _arcgis_field(field) for field in table.schema if field.name not in {"bbox", "geometry"}
    ]
    features: list[dict[str, Any]] = []
    for row in table.to_pylist():
        geometry_wkb = row.pop("geometry")
        row.pop("bbox", None)
        geometry = None if geometry_wkb is None else mapping(load_wkb(geometry_wkb))
        features.append(
            {
                "type": "Feature",
                "id": row["objectid"],
                "properties": row,
                "geometry": geometry,
            }
        )
    return Layer(
        service_name=service_name,
        layer_name=layer_name,
        geometry_type=geometry_type,
        description=description,
        features=features,
        fields=fields,
    )


def _renderer(geometry_type: str) -> dict[str, Any]:
    """Return a valid ArcGIS renderer for style extraction."""
    if geometry_type == "esriGeometryPoint":
        symbol = {
            "type": "esriSMS",
            "style": "esriSMSCircle",
            "color": [0, 122, 194, 210],
            "size": 7,
            "outline": {"color": [255, 255, 255, 255], "width": 1},
        }
    else:
        symbol = {
            "type": "esriSFS",
            "style": "esriSFSSolid",
            "color": [0, 122, 194, 70],
            "outline": {"color": [0, 71, 112, 255], "width": 1},
        }
    return {"type": "simple", "symbol": symbol}


def _service_payload(layer: Layer) -> dict[str, Any]:
    """Build a FeatureServer discovery response."""
    return {
        "currentVersion": 12,
        "serviceDescription": layer.description,
        "description": layer.description,
        "copyrightText": "City of Philadelphia",
        "layers": [{"id": 0, "name": layer.layer_name, "type": "Feature Layer"}],
        "tables": [],
    }


def _layer_payload(layer: Layer) -> dict[str, Any]:
    """Build a layer metadata response."""
    return {
        "currentVersion": 12,
        "id": 0,
        "name": layer.layer_name,
        "type": "Feature Layer",
        "description": layer.description,
        "copyrightText": "City of Philadelphia",
        "geometryType": layer.geometry_type,
        "objectIdField": "objectid",
        "maxRecordCount": 100,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {
            "xmin": -75.3,
            "ymin": 39.8,
            "xmax": -74.9,
            "ymax": 40.2,
            "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        },
        "fields": layer.fields,
        "drawingInfo": {"renderer": _renderer(layer.geometry_type)},
    }


class PhiladelphiaArcGISServer(ThreadingHTTPServer):
    """Threaded server with immutable fixtures and a request audit log."""

    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        layers: dict[str, Layer],
        request_log: Path,
    ) -> None:
        super().__init__(server_address, PhiladelphiaArcGISHandler)
        self.layers = layers
        self.request_log = request_log
        self.log_lock = threading.Lock()
        self.failure_lock = threading.Lock()
        self.failed_offset_200 = False

    def record(self, path: str, query: dict[str, list[str]], status: int) -> None:
        """Append one request to the JSON Lines audit log."""
        entry = json.dumps({"path": path, "query": query, "status": status}, sort_keys=True)
        with self.log_lock:
            with self.request_log.open("a", encoding="utf-8") as stream:
                stream.write(f"{entry}\n")

    def should_fail_once(self, path: str, query: dict[str, list[str]]) -> bool:
        """Inject one retryable failure into the third affordable-housing page."""
        if "AffordableHousingProduction" not in path:
            return False
        if query.get("resultOffset") != ["200"]:
            return False
        with self.failure_lock:
            if self.failed_offset_200:
                return False
            self.failed_offset_200 = True
            return True


class PhiladelphiaArcGISHandler(BaseHTTPRequestHandler):
    """Serve the ArcGIS REST subset used by ``portolan extract arcgis``."""

    server: PhiladelphiaArcGISServer

    def log_message(self, format: str, *args: object) -> None:
        """Disable stderr access logs; tests use the structured request log."""

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        """Handle one ArcGIS discovery, metadata, count, or page request."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        query = parse_qs(parsed.query)

        if self.server.should_fail_once(path, query):
            self.server.record(path, query, 503)
            self._send_json(
                {"error": {"code": 503, "message": "Temporary fixture failure"}},
                status=503,
            )
            return

        payload = self._response(path, query)
        status = 200 if payload is not None else 404
        self.server.record(path, query, status)
        if payload is None:
            self._send_json({"error": {"code": 404, "message": "Not found"}}, status=404)
            return
        self._send_json(payload)

    def _response(
        self,
        path: str,
        query: dict[str, list[str]],
    ) -> dict[str, Any] | None:
        root = "/ArcGIS/rest/services"
        if path == root:
            services = [
                {"name": name, "type": "FeatureServer"} for name in sorted(self.server.layers)
            ]
            services.append({"name": "Unrelated_Parks", "type": "FeatureServer"})
            return {"currentVersion": 12, "services": services, "folders": []}

        for service_name, layer in self.server.layers.items():
            service_path = f"{root}/{service_name}/FeatureServer"
            layer_path = f"{service_path}/0"
            if path == service_path:
                return _service_payload(layer)
            if path == layer_path:
                return _layer_payload(layer)
            if path == f"{layer_path}/query":
                if query.get("returnCountOnly") == ["true"]:
                    return {"count": len(layer.features)}
                offset = int(query.get("resultOffset", ["0"])[0])
                count = int(query.get("resultRecordCount", ["100"])[0])
                return {
                    "type": "FeatureCollection",
                    "features": layer.features[offset : offset + count],
                }
        return None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture-dir", type=Path, required=True)
    parser.add_argument("--port-file", type=Path, required=True)
    parser.add_argument("--request-log", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    """Load the pinned Assets and serve them until the test stops the process."""
    args = _parse_args()
    layers = {
        "AffordableHousingProduction": _load_layer(
            args.fixture_dir / "affordable_housing.parquet",
            service_name="AffordableHousingProduction",
            layer_name="Affordable_Housing",
            geometry_type="esriGeometryPoint",
            description=(
                "Affordable housing projects funded by Philadelphia's Division of Housing "
                "and Community Development."
            ),
        ),
        "Council_Districts_2024": _load_layer(
            args.fixture_dir / "council_districts_2024.parquet",
            service_name="Council_Districts_2024",
            layer_name="Council_Districts_2024",
            geometry_type="esriGeometryPolygon",
            description="Philadelphia City Council districts redrawn after the 2020 census.",
        ),
    }
    server = PhiladelphiaArcGISServer(("127.0.0.1", 0), layers, args.request_log)
    port = int(server.server_address[1])
    args.port_file.write_text(str(port), encoding="utf-8")
    server.serve_forever()


if __name__ == "__main__":
    main()
