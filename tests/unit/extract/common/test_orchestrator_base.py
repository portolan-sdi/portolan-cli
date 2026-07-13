"""Tests for the shared post-extraction catalog lifecycle.

These cover the helpers in ``extract/common/orchestrator_base.py`` that the
ArcGIS, WFS, and Carto orchestrators delegate to. The source-specific pieces
(URL/title builders, metadata serializers) are exercised via the orchestrators'
own suites; here we verify the shared skeleton in isolation.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.extract.common.orchestrator_base import (
    add_source_links,
    collect_successful_parquet_files,
    init_extracted_catalog,
    register_collection_styles,
    seed_catalog_metadata,
)
from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
)
from portolan_cli.metadata_extraction import ExtractedMetadata

pytestmark = pytest.mark.unit


def _layer(
    layer_id: int,
    name: str,
    *,
    status: str = "success",
    output_path: str | None = None,
) -> LayerResult:
    return LayerResult(
        id=layer_id,
        name=name,
        status=status,
        features=1,
        size_bytes=1,
        duration_seconds=0.0,
        output_path=output_path,
        warnings=[],
        error=None,
        attempts=1,
    )


def _report(
    layers: list[LayerResult], *, source_url: str = "https://example.com/svc"
) -> ExtractionReport:
    return ExtractionReport(
        extraction_date="2026-07-09T00:00:00+00:00",
        source_url=source_url,
        portolan_version="test",
        gpio_version="test",
        metadata_extracted=None,
        layers=layers,
        summary=ExtractionSummary(
            total_layers=len(layers),
            succeeded=sum(1 for r in layers if r.status == "success"),
            failed=0,
            skipped=0,
            empty=0,
            total_features=0,
            total_size_bytes=0,
            total_duration_seconds=0.0,
        ),
    )


class TestCollectSuccessfulParquetFiles:
    def test_includes_only_successful_with_output(self) -> None:
        report = _report(
            [
                _layer(0, "a", output_path="a/a.parquet"),
                _layer(1, "b", status="failed"),
                _layer(2, "c", status="empty"),
                _layer(3, "d", status="success", output_path=None),
                _layer(4, "e", output_path="e/e.parquet"),
            ]
        )
        files = collect_successful_parquet_files(Path("/out"), report)
        assert files == [Path("/out/a/a.parquet"), Path("/out/e/e.parquet")]

    def test_empty_when_nothing_succeeded(self) -> None:
        report = _report([_layer(0, "a", status="failed")])
        assert collect_successful_parquet_files(Path("/out"), report) == []


class TestInitExtractedCatalog:
    def test_returns_none_and_skips_hooks_when_no_assets(self, tmp_path: Path) -> None:
        report = _report([_layer(0, "a", status="failed")])
        called = {"post_init": False}

        def _post_init(_out: Path, _files: list[Path]) -> None:
            called["post_init"] = True

        result = init_extracted_catalog(
            tmp_path, report, title="t", description="d", post_init=_post_init
        )

        assert result is None
        assert called["post_init"] is False
        # No catalog was created.
        assert not (tmp_path / "catalog.json").exists()

    def test_runs_post_init_between_init_and_add(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        report = _report([_layer(0, "a", output_path="a/a.parquet")])
        events: list[str] = []

        import portolan_cli.catalog as catalog_mod

        def _fake_init(output_dir: Path, *, title: str | None, description: str | None) -> None:
            events.append("init")

        def _fake_add(*, paths: list[Path], catalog_root: Path) -> None:
            events.append("add")

        def _post_init(_out: Path, files: list[Path]) -> None:
            events.append("post_init")
            assert files == [tmp_path / "a/a.parquet"]

        monkeypatch.setattr(catalog_mod, "init_catalog", _fake_init)
        monkeypatch.setattr(catalog_mod, "add_files", _fake_add)

        result = init_extracted_catalog(
            tmp_path, report, title="t", description="d", post_init=_post_init
        )

        assert result == [tmp_path / "a/a.parquet"]
        assert events == ["init", "post_init", "add"]

    def test_post_init_optional(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        report = _report([_layer(0, "a", output_path="a/a.parquet")])
        import portolan_cli.catalog as catalog_mod

        monkeypatch.setattr(catalog_mod, "init_catalog", lambda *a, **k: None)
        monkeypatch.setattr(catalog_mod, "add_files", lambda *a, **k: None)

        result = init_extracted_catalog(tmp_path, report, title=None, description=None)
        assert result == [tmp_path / "a/a.parquet"]


class TestAddSourceLinks:
    def test_adds_link_only_for_successful_with_collection_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Two successful layers, but only one has a collection.json on disk.
        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "collection.json").write_text("{}")
        # layer b's collection.json is missing -> skipped.
        (tmp_path / "b").mkdir()

        report = _report(
            [
                _layer(0, "a", output_path="a/a.parquet"),
                _layer(1, "b", output_path="b/b.parquet"),
                _layer(2, "c", status="failed"),
            ]
        )

        calls: list[tuple[Path, str, str]] = []

        import portolan_cli.stac as stac_mod

        def _fake_add_via_link(path: Path, url: str, *, title: str) -> None:
            calls.append((path, url, title))

        monkeypatch.setattr(stac_mod, "add_via_link", _fake_add_via_link)

        add_source_links(
            tmp_path,
            report,
            url_builder=lambda source_url, layer: f"{source_url}/{layer.id}",
            title_builder=lambda layer: f"Source: {layer.name}",
        )

        assert calls == [
            (
                tmp_path / "a" / "collection.json",
                "https://example.com/svc/0",
                "Source: a",
            )
        ]


class TestRegisterCollectionStyles:
    def test_registers_styles_and_optional_legends(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        report = _report([_layer(0, "a", output_path="a/a.parquet")])

        import portolan_cli.viz.style as style_mod

        style_calls: list[Path] = []
        legend_calls: list[Path] = []

        monkeypatch.setattr(style_mod, "discover_styles", lambda d: ["style"])
        monkeypatch.setattr(style_mod, "register_style_assets", lambda d, s: style_calls.append(d))
        monkeypatch.setattr(style_mod, "discover_legends", lambda d: ["legend"])
        monkeypatch.setattr(
            style_mod, "register_legend_assets", lambda d, s: legend_calls.append(d)
        )

        # Without legends
        register_collection_styles(tmp_path, report)
        assert style_calls == [tmp_path / "a"]
        assert legend_calls == []

        # With legends
        register_collection_styles(tmp_path, report, include_legends=True)
        assert legend_calls == [tmp_path / "a"]

    def test_skips_unsuccessful_layers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        report = _report([_layer(0, "a", status="failed")])
        import portolan_cli.viz.style as style_mod

        called = {"discover": False}

        def _discover(_d: Path) -> list[str]:
            called["discover"] = True
            return []

        monkeypatch.setattr(style_mod, "discover_styles", _discover)
        monkeypatch.setattr(style_mod, "register_style_assets", lambda d, s: None)

        register_collection_styles(tmp_path, report)
        assert called["discover"] is False


class TestSeedCatalogMetadata:
    def test_none_is_noop(self, tmp_path: Path) -> None:
        seed_catalog_metadata(tmp_path, None)
        assert not (tmp_path / ".portolan" / "metadata.yaml").exists()

    def test_writes_metadata_yaml(self, tmp_path: Path) -> None:
        (tmp_path / ".portolan").mkdir()
        extracted = ExtractedMetadata(
            source_type="test_source",
            source_url="https://example.com/svc",
            description="A test service",
        )
        seed_catalog_metadata(tmp_path, extracted)
        assert (tmp_path / ".portolan" / "metadata.yaml").exists()
