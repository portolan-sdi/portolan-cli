"""Unit tests for the Carto extraction orchestrator."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.extract.carto import orchestrator
from portolan_cli.extract.carto.discovery import (
    CartoDiscoveryError,
    CartoDiscoveryResult,
    CartoTableInfo,
)
from portolan_cli.extract.carto.orchestrator import (
    ExtractionOptions,
    _build_report,
    extract_carto_catalog,
)
from portolan_cli.extract.common.report import LayerResult, save_report

pytestmark = [pytest.mark.unit]


def _result(tables: list[CartoTableInfo]) -> CartoDiscoveryResult:
    return CartoDiscoveryResult(
        service_url="https://phl.carto.com/api/v2/sql", tables=tables, account_name="phl"
    )


def _patch_discovery(monkeypatch: pytest.MonkeyPatch, tables: list[CartoTableInfo]) -> None:
    """Patch enumeration (names) and geometry resolution (per-name lookup)."""
    geom_by_name = {t.name: t.has_geometry for t in tables}
    name_only = [CartoTableInfo(t.name, id=t.id) for t in tables]
    monkeypatch.setattr(orchestrator, "discover_carto_tables", lambda *a, **k: _result(name_only))
    monkeypatch.setattr(
        orchestrator, "table_has_geometry", lambda url, name, **k: geom_by_name[name]
    )


def test_extracts_non_spatial_tables_as_tabular(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Non-geo tables are extracted as plain Parquet (geometry=False), not skipped.

    gpio's ``geometry`` flag is threaded per table; ``bbox`` is dropped for the
    non-geo table because a bounding-box filter requires a geometry column.
    """
    _patch_discovery(
        monkeypatch,
        [
            CartoTableInfo("parcels", id=0, has_geometry=True),
            CartoTableInfo("lookup", id=1, has_geometry=False),
        ],
    )
    calls: dict[str, dict[str, Any]] = {}

    def fake_convert(**kwargs: Any) -> None:
        calls[kwargs["table_name"]] = kwargs
        pq.write_table(pa.table({"a": [1]}), kwargs["output_file"])

    import geoparquet_io.core.carto as gpio_carto  # type: ignore[import-untyped]

    monkeypatch.setattr(gpio_carto, "convert_carto_to_geoparquet", fake_convert)

    report = extract_carto_catalog(
        "https://phl.carto.com",
        tmp_path,
        options=ExtractionOptions(raw=True, bbox=(0.0, 0.0, 1.0, 1.0)),
    )

    # Both tables extracted; geometry flag (and bbox handling) threaded per table.
    assert calls["parcels"]["geometry"] is True
    assert calls["parcels"]["bbox"] == (0.0, 0.0, 1.0, 1.0)
    assert calls["lookup"]["geometry"] is False
    assert calls["lookup"]["bbox"] is None  # bbox needs geometry; dropped for tabular
    by_name = {layer.name: layer.status for layer in report.layers}
    assert by_name["parcels"] == "success"
    assert by_name["lookup"] == "success"
    assert report.summary.succeeded == 2
    assert report.summary.skipped == 0


def test_threads_options_into_gpio(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _patch_discovery(monkeypatch, [CartoTableInfo("parcels", id=0, has_geometry=True)])
    captured: dict[str, Any] = {}

    def fake_convert(**kwargs: Any) -> None:
        captured.update(kwargs)
        pq.write_table(pa.table({"a": [1]}), kwargs["output_file"])

    import geoparquet_io.core.carto as gpio_carto  # type: ignore[import-untyped]

    monkeypatch.setattr(gpio_carto, "convert_carto_to_geoparquet", fake_convert)

    extract_carto_catalog(
        "https://phl.carto.com",
        tmp_path,
        options=ExtractionOptions(
            raw=True, where="updated_at > '2026-01-01'", limit=5, api_key="secret"
        ),
    )

    assert captured["table_name"] == "parcels"
    assert captured["where"] == "updated_at > '2026-01-01'"
    assert captured["limit"] == 5
    assert captured["api_key"] == "secret"
    assert captured["url"] == "https://phl.carto.com/api/v2/sql"
    assert captured["geometry"] is True  # spatial table → GeoParquet path


def test_dry_run_marks_pending_without_extracting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_discovery(monkeypatch, [CartoTableInfo("parcels", id=0, has_geometry=True)])

    def fail_single(*a: Any, **k: Any) -> tuple[int, int, float]:
        raise AssertionError("extraction must not run during dry-run")

    monkeypatch.setattr(orchestrator, "_extract_single_table", fail_single)

    report = extract_carto_catalog(
        "https://phl.carto.com", tmp_path, options=ExtractionOptions(dry_run=True)
    )
    assert [layer.status for layer in report.layers] == ["pending"]
    assert not (tmp_path / "catalog.json").exists()


def test_fallback_to_explicit_tables_on_discovery_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def boom(*a: Any, **k: Any) -> CartoDiscoveryResult:
        raise CartoDiscoveryError("CDB_UserTables unavailable")

    monkeypatch.setattr(orchestrator, "discover_carto_tables", boom)
    monkeypatch.setattr(
        orchestrator,
        "tables_from_names",
        lambda url, names, **k: _result([CartoTableInfo(names[0], id=0)]),
    )
    monkeypatch.setattr(orchestrator, "table_has_geometry", lambda url, name, **k: True)
    monkeypatch.setattr(orchestrator, "_extract_single_table", lambda *a, **k: (1, 10, 0.1))

    report = extract_carto_catalog(
        "https://phl.carto.com",
        tmp_path,
        layer_filter=["my_table"],
        options=ExtractionOptions(raw=True),
    )
    assert report.summary.succeeded == 1
    assert report.layers[0].name == "my_table"


def test_discovery_error_without_explicit_tables_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def boom(*a: Any, **k: Any) -> CartoDiscoveryResult:
        raise CartoDiscoveryError("CDB_UserTables unavailable")

    monkeypatch.setattr(orchestrator, "discover_carto_tables", boom)
    with pytest.raises(CartoDiscoveryError):
        extract_carto_catalog(
            "https://phl.carto.com", tmp_path, options=ExtractionOptions(raw=True)
        )


def test_resume_skips_previously_succeeded_tables(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_discovery(
        monkeypatch,
        [
            CartoTableInfo("done", id=0, has_geometry=True),
            CartoTableInfo("todo", id=1, has_geometry=True),
        ],
    )
    # Seed a prior report marking "done" as already extracted.
    prior = _build_report(
        "https://phl.carto.com/api/v2/sql",
        "phl",
        [
            LayerResult(
                id=0,
                name="done",
                status="success",
                features=1,
                size_bytes=1,
                duration_seconds=0.1,
                output_path="done/done.parquet",
                warnings=[],
                error=None,
                attempts=1,
            )
        ],
    )
    save_report(prior, tmp_path / ".portolan" / "extraction-report.json")

    extracted: list[str] = []

    def fake_extract(url: str, table: Any, path: Path, opts: Any) -> tuple[int, int, float]:
        extracted.append(table.name)
        return (1, 10, 0.1)

    monkeypatch.setattr(orchestrator, "_extract_single_table", fake_extract)

    report = extract_carto_catalog(
        "https://phl.carto.com", tmp_path, options=ExtractionOptions(resume=True, raw=True)
    )

    assert extracted == ["todo"]  # "done" is skipped on resume
    by_name = {layer.name: layer.status for layer in report.layers}
    assert by_name["done"] == "skipped"
    assert by_name["todo"] == "success"


def test_workers_gt_one_extracts_all_tables(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_discovery(
        monkeypatch,
        [CartoTableInfo(name, id=i, has_geometry=True) for i, name in enumerate("abc")],
    )
    extracted: list[str] = []

    def fake_extract(url: str, table: Any, path: Path, opts: Any) -> tuple[int, int, float]:
        extracted.append(table.name)
        return (1, 10, 0.1)

    monkeypatch.setattr(orchestrator, "_extract_single_table", fake_extract)

    report = extract_carto_catalog(
        "https://phl.carto.com", tmp_path, options=ExtractionOptions(workers=3, raw=True)
    )

    assert sorted(extracted) == ["a", "b", "c"]  # parallel path extracts every table
    assert report.summary.succeeded == 3
