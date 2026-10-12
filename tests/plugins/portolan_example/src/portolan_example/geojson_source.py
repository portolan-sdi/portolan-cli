"""The ``example`` extractor: each GeoJSON file in a directory is a layer.

It implements ``LayerSource``. ``portolan_cli.api.extractor_from`` gives it
filters, resume, retry, the report, and catalog setup.
"""

from __future__ import annotations

from pathlib import Path

from portolan_cli.api import ExtractedMetadata, LayerMetadata, LayerRef


class GeoJSONDirectory:
    """Layers from the ``*.geojson`` files in the directory ``source``."""

    name = "example"
    summary = "Layers from a directory of GeoJSON files."

    def discover(self, source: str) -> list[LayerRef]:
        """One layer per file, in name order."""
        files = sorted(Path(source).glob("*.geojson"))
        return [LayerRef(id=index, name=path.stem) for index, path in enumerate(files)]

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        """Convert the layer's file to GeoParquet with geoparquet-io."""
        import geoparquet_io as gpio  # type: ignore[import-untyped]

        table = gpio.convert(str(Path(source) / f"{layer.name}.geojson"))
        table.write(str(dest))
        return int(table.num_rows)

    def service_metadata(self, source: str) -> ExtractedMetadata:
        """Describe the directory. It publishes no license."""
        return ExtractedMetadata(
            source_type=self.name,
            source_url=Path(source).resolve().as_uri(),
            description=f"GeoJSON files from the directory {Path(source).name}.",
        )

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata:
        """Title each collection after its file."""
        return LayerMetadata(title=layer.name.replace("_", " ").capitalize())

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        """The example writes no styles."""

    def source_link(self, source: str, layer: LayerRef) -> str:
        """Link each collection to its file."""
        return (Path(source) / f"{layer.name}.geojson").resolve().as_uri()
