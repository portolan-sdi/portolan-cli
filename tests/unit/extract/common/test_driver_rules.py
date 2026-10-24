"""The shared driver rules of #1034, with fake LayerSource objects and no network.

Rule 1: a layer the user names that returns 0 features is ``failed`` (#963).
Rule 2: a run that selects 0 layers raises, and the message names the cause
(#652, #812). Rule 3: a filter pattern that matches no layer warns (#768).
Rule 4: an empty layer from bulk discovery stays ``empty``, with a warning.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from portolan_cli.extract.common.source import (
    Discovery,
    EmptyLayerError,
    LayerMetadata,
    LayerRef,
    LayerSourceOptions,
    NoLayersSelectedError,
    SourceLink,
    extractor_from,
    filter_refs,
    run_layer_extraction,
)

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.extract.common.progress import ExtractionProgress

pytestmark = pytest.mark.unit

_WFS_NAMES = ["cadastru_data:terenuri", "cadastru_data:UAT1", "cadastru_data:cladiri"]


class _FakeSource:
    """Layers from a list. ``empty`` names layers whose fetch raises EmptyLayerError."""

    name = "fake"
    summary = "Fake layers."

    def __init__(
        self,
        layers: list[LayerRef],
        *,
        empty: set[str] | None = None,
        empty_reason: str | None = None,
    ) -> None:
        self.layers = layers
        self.empty = empty or set()
        self.empty_reason = empty_reason
        self.fetched: list[str] = []

    def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
        return Discovery(source_url=source, layers=self.layers, empty_reason=self.empty_reason)

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        self.fetched.append(layer.name)
        if layer.name in self.empty:
            raise EmptyLayerError(f"No features returned for {layer.name}")
        pq.write_table(pa.table({"value": [1, 2]}), dest)
        return 2

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata | None:
        return None

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        return None

    def source_link(self, source: str, layer: LayerRef) -> SourceLink | None:
        return None


def _refs(names: list[str], ids: list[int] | None = None) -> list[LayerRef]:
    return [
        LayerRef(id=layer_id, name=name)
        for layer_id, name in zip(ids or range(len(names)), names, strict=True)
    ]


def _options(**fields: Any) -> LayerSourceOptions:
    return LayerSourceOptions(**{"raw": True, "retry_delay": 0.0, **fields})


class TestFilterRefs:
    def test_numeric_pattern_matches_the_source_id_not_the_position(self) -> None:
        """#1003: --layers 2 selects layer id 2 in a service with ids 0, 2, 5."""
        layers = _refs(["a", "b", "c"], ids=[0, 2, 5])

        kept = filter_refs(layers, ["2"], None)

        assert [(ref.id, ref.name) for ref in kept] == [(2, "b")]

    def test_plain_pattern_matches_the_leaf_of_a_qualified_name(self) -> None:
        """#768/#1034: --exclude-layers terenuri excludes cadastru_data:terenuri."""
        kept = filter_refs(_refs(_WFS_NAMES), None, ["terenuri"])

        assert [ref.name for ref in kept] == ["cadastru_data:UAT1", "cadastru_data:cladiri"]

    def test_pattern_that_matches_nothing_warns_with_three_names(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """#1034 rule 3: the warning names the pattern and 3 layers of the source."""
        layers = _refs([*_WFS_NAMES, "cadastru_data:adresa_point"])

        with caplog.at_level(logging.WARNING, logger="portolan_cli.extract.common.source"):
            kept = filter_refs(layers, None, ["teren"])

        assert len(kept) == 4
        assert [record.getMessage() for record in caplog.records] == [
            "Layer pattern 'teren' matches no layer. Layers at the source include: "
            "cadastru_data:terenuri, cadastru_data:UAT1, cadastru_data:cladiri"
        ]

    def test_id_pattern_that_matches_an_id_does_not_warn(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="portolan_cli.extract.common.source"):
            kept = filter_refs(_refs(["a", "b"], ids=[3, 7]), ["7"], None)

        assert ([ref.name for ref in kept], caplog.records) == (["b"], [])

    def test_a_source_with_no_layers_warns_with_none(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="portolan_cli.extract.common.source"):
            filter_refs([], ["roads"], None)

        assert [record.getMessage() for record in caplog.records] == [
            "Layer pattern 'roads' matches no layer. Layers at the source include: none"
        ]

    def test_named_layer_is_explicit_and_glob_match_is_not(self) -> None:
        kept = filter_refs(_refs(_WFS_NAMES), ["UAT1", "*:clad*"], None)

        assert [(ref.name, ref.explicit) for ref in kept] == [
            ("cadastru_data:UAT1", True),
            ("cadastru_data:cladiri", False),
        ]


class TestNamedLayers:
    def test_named_layer_with_zero_features_fails(self, tmp_path: Path) -> None:
        """#963/#1034 rule 1: a layer named with --layers that is empty is failed."""
        source = _FakeSource(_refs(_WFS_NAMES), empty={"cadastru_data:UAT1"})

        report = run_layer_extraction(
            source, "https://h.example/wfs", tmp_path, _options(layer_filter=["UAT1"])
        )

        assert [(r.name, r.status) for r in report.layers] == [("cadastru_data:UAT1", "failed")]
        assert report.layers[0].error == (
            "Layer cadastru_data:UAT1 returned 0 features, and the run named it: "
            "No features returned for cadastru_data:UAT1"
        )
        assert report.summary.failed == 1

    def test_layer_url_layer_with_zero_features_fails(self, tmp_path: Path) -> None:
        """A source marks a layer from a layer URL as explicit (#652)."""
        layer = LayerRef(id=0, name="PRK.open_space", explicit=True)
        source = _FakeSource([layer], empty={"PRK.open_space"})

        report = run_layer_extraction(source, "https://h.example/MapServer/0", tmp_path, _options())

        assert [r.status for r in report.layers] == ["failed"]

    def test_empty_layer_is_not_retried(self, tmp_path: Path) -> None:
        source = _FakeSource(_refs(["a"]), empty={"a"})

        report = run_layer_extraction(source, "https://h.example", tmp_path, _options(retries=3))

        assert source.fetched == ["a"]
        assert report.layers[0].attempts == 1


class TestZeroLayers:
    def test_filter_that_selects_nothing_raises(self, tmp_path: Path) -> None:
        """#1034 rule 2: the source advertises layers, and the run selects 0."""
        source = _FakeSource(_refs(_WFS_NAMES))

        with pytest.raises(NoLayersSelectedError) as caught:
            run_layer_extraction(
                source, "https://h.example/wfs", tmp_path, _options(layer_filter=["roads"])
            )

        assert caught.value.message == (
            "No layer at https://h.example/wfs matches the layer filters. "
            "The source lists 3 layers."
        )
        assert source.fetched == []

    def test_source_reason_names_the_cause(self, tmp_path: Path) -> None:
        """#812: a source with only tables says how to reach them."""
        reason = "The service lists 1 table and no layers. Pass --include-tables."
        source = _FakeSource([], empty_reason=reason)

        with pytest.raises(NoLayersSelectedError) as caught:
            run_layer_extraction(source, "https://h.example/FeatureServer", tmp_path, _options())

        assert caught.value.message == (
            "No layers to extract from https://h.example/FeatureServer. " + reason
        )

    def test_dry_run_also_raises(self, tmp_path: Path) -> None:
        with pytest.raises(NoLayersSelectedError):
            run_layer_extraction(
                _FakeSource([]), "https://h.example", tmp_path, _options(dry_run=True)
            )


class TestFetchRetry:
    def test_a_fetch_that_writes_zero_rows_is_empty(self, tmp_path: Path) -> None:
        class _ZeroRows(_FakeSource):
            def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
                self.fetched.append(layer.name)
                pq.write_table(pa.table({"value": pa.array([], pa.int64())}), dest)
                return 0

        source = _ZeroRows(_refs(["a"]))

        report = run_layer_extraction(source, "https://h.example", tmp_path, _options(retries=3))

        assert (source.fetched, report.layers[0].status) == (["a"], "empty")

    def test_a_failure_is_retried_and_the_report_counts_the_attempts(self, tmp_path: Path) -> None:
        class _Flaky(_FakeSource):
            def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
                if layer.name == "down" or layer.name not in self.fetched:
                    self.fetched.append(layer.name)
                    raise OSError(f"{layer.name} is down")
                return super().fetch(source, layer, dest)

        source = _Flaky(_refs(["flaky", "down"]))

        report = run_layer_extraction(source, "https://h.example", tmp_path, _options(retries=2))

        assert source.fetched == ["flaky", "flaky", "down", "down"]
        assert [(r.name, r.status, r.attempts, r.error) for r in report.layers] == [
            ("flaky", "success", 2, None),
            ("down", "failed", 2, "down is down"),
        ]

    def test_zero_retries_still_fetches_once(self, tmp_path: Path) -> None:
        class _Down(_FakeSource):
            def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
                self.fetched.append(layer.name)
                raise OSError("down")

        source = _Down(_refs(["a"]))

        run_layer_extraction(source, "https://h.example", tmp_path, _options(retries=0))

        assert source.fetched == ["a"]

    def test_styles_move_into_the_collection(self, tmp_path: Path) -> None:
        class _Styled(_FakeSource):
            def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
                (collection_dir / "styles").mkdir()
                (collection_dir / "styles" / "default.json").write_text("{}")

        run_layer_extraction(_Styled(_refs(["a"])), "https://h.example", tmp_path, _options())

        assert (tmp_path / "a" / "styles" / "default.json").read_text() == "{}"


class TestBulkDiscovery:
    def test_unnamed_empty_layer_stays_empty_with_a_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """#1034 rule 4: bulk discovery keeps the empty status."""
        source = _FakeSource(_refs(["a", "b"]), empty={"b"})

        with caplog.at_level(logging.WARNING, logger="portolan_cli.extract.common.source"):
            report = run_layer_extraction(source, "https://h.example", tmp_path, _options())

        assert [(r.name, r.status) for r in report.layers] == [("a", "success"), ("b", "empty")]
        assert report.layers[1].warnings == ["Layer has no features"]
        assert "Layer b is empty (0 features), skipping" in caplog.messages


class TestProgressOrder:
    def test_index_only_increases_with_skipped_and_resumed_layers(self, tmp_path: Path) -> None:
        """#1012: skipped and resumed layers print at their own position."""
        layers = [
            LayerRef(id=0, name="done"),
            LayerRef(id=1, name="fetch1"),
            LayerRef(id=2, name="tiles", skip_reason="skipped: no Query capability (tiles-only)"),
            LayerRef(id=3, name="fetch2"),
        ]
        run_layer_extraction(_FakeSource([layers[0]]), "https://h.example", tmp_path, _options())
        events: list[ExtractionProgress] = []

        run_layer_extraction(
            _FakeSource(layers),
            "https://h.example",
            tmp_path,
            _options(resume=True),
            on_progress=events.append,
        )

        starts = [event.layer_index for event in events if event.status == "starting"]
        assert starts == [0, 1, 2, 3]
        assert [(e.layer_name, e.status) for e in events if e.status != "extracting"] == [
            ("done", "starting"),
            ("done", "skipped"),
            ("fetch1", "starting"),
            ("fetch1", "success"),
            ("tiles", "starting"),
            ("tiles", "skipped"),
            ("fetch2", "starting"),
            ("fetch2", "success"),
        ]


class TestRunState:
    def test_parallel_run_reports_and_keeps_the_skipped_layer(self, tmp_path: Path) -> None:
        """With workers, a skipped layer reports first at its own index and stays in the report."""
        layers = [
            LayerRef(id=0, name="tiles", skip_reason="skipped: tiles-only"),
            LayerRef(id=1, name="a"),
            LayerRef(id=2, name="b"),
        ]
        events: list[ExtractionProgress] = []

        report = run_layer_extraction(
            _FakeSource(layers),
            "https://h.example",
            tmp_path,
            _options(workers=2),
            on_progress=events.append,
        )

        assert [(e.layer_index, e.total_layers, e.layer_name, e.status) for e in events[:2]] == [
            (0, 3, "tiles", "starting"),
            (0, 3, "tiles", "skipped"),
        ]
        assert {e.total_layers for e in events} == {3}
        assert [(r.name, r.status) for r in report.layers] == [
            ("tiles", "skipped"),
            ("a", "success"),
            ("b", "success"),
        ]

    def test_a_run_without_resume_fetches_again_and_leaves_no_staging(self, tmp_path: Path) -> None:
        run_layer_extraction(_FakeSource(_refs(["a"])), "https://h.example", tmp_path, _options())
        again = _FakeSource(_refs(["a"]))

        report = run_layer_extraction(again, "https://h.example", tmp_path, _options())

        assert again.fetched == ["a"]
        assert not (tmp_path / ".portolan" / "staging").exists()
        assert report.summary.wall_duration_seconds is not None
        assert 0 <= report.summary.wall_duration_seconds < 60


class TestExitCode:
    def _run(self, source: _FakeSource, tmp_path: Path, *args: str) -> Any:
        command = extractor_from(source).cli_command()
        return CliRunner().invoke(
            command, ["https://h.example/wfs", str(tmp_path / "out"), "--raw", "--auto", *args]
        )

    def test_named_empty_layer_exits_non_zero(self, tmp_path: Path) -> None:
        source = _FakeSource(_refs(_WFS_NAMES), empty={"cadastru_data:UAT1"})

        result = self._run(source, tmp_path, "--layers", "UAT1")

        assert result.exit_code == 1

    def test_zero_selected_layers_exits_non_zero_with_the_cause(self, tmp_path: Path) -> None:
        result = self._run(_FakeSource(_refs(_WFS_NAMES)), tmp_path, "--layers", "roads")

        assert result.exit_code == 1
        assert "matches the layer filters" in result.output


@pytest.mark.integration
def test_credentials_never_reach_the_catalog(fixtures_dir: Path, tmp_path: Path) -> None:
    """#981: an API key in the source URL is in no file the extraction writes."""
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    from portolan_cli.metadata_extraction import ExtractedMetadata

    points = fixtures_dir / "vector" / "valid" / "points.geojson"
    url = "https://h.example/geoserver/wfs?SERVICE=WFS&apikey=SECRET"

    class _KeyedSource(_FakeSource):
        def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
            return Discovery(
                source_url=source,
                layers=self.layers,
                catalog_seed=ExtractedMetadata(source_type="fake", source_url=source),
            )

        def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
            table = gpio.convert(str(points))
            table.write(str(dest))
            return int(table.num_rows)

        def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata | None:
            return LayerMetadata(title="Points", source_url=f"{source}&typename={layer.name}")

        def source_link(self, source: str, layer: LayerRef) -> SourceLink | None:
            return SourceLink(href=f"{source}&typename={layer.name}", title="Source")

    out = tmp_path / "out"
    run_layer_extraction(
        _KeyedSource(_refs(["ws:points"])), url, out, _options(raw=False, license="CC0-1.0")
    )

    leaks = [
        path.relative_to(out).as_posix()
        for path in sorted(out.rglob("*"))
        if path.is_file() and b"SECRET" in path.read_bytes()
    ]
    assert leaks == []
    collection = json.loads((out / "ws_points" / "collection.json").read_text(encoding="utf-8"))
    vias = [link["href"] for link in collection["links"] if link["rel"] == "via"]
    assert vias == ["https://h.example/geoserver/wfs?SERVICE=WFS&typename=ws%3Apoints"]


def test_a_failed_layer_error_keeps_no_credential(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """#981: an error page that echoes the request URL leaves its API key out of every message.

    The report, the progress event that the terminal prints, and the retry log
    each get the error without the key.
    """
    url = "https://h.example/geoserver/wfs?authkey=SECRET"
    clean = "Server returned an error page:\n<p>Blocked: https://h.example/geoserver/wfs?n=1</p>"

    class _BlockedSource(_FakeSource):
        def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
            raise RuntimeError(f"Server returned an error page:\n<p>Blocked: {source}&n=1</p>")

    events: list[ExtractionProgress] = []
    with caplog.at_level(logging.DEBUG, logger="portolan_cli.extract"):
        run_layer_extraction(
            _BlockedSource(_refs(["roads"])),
            url,
            tmp_path,
            _options(retries=2),
            on_progress=events.append,
        )

    report = json.loads((tmp_path / ".portolan" / "extraction-report.json").read_text())
    assert [(layer["status"], layer["error"]) for layer in report["layers"]] == [("failed", clean)]
    assert [(e.status, e.error) for e in events if e.status == "failed"] == [("failed", clean)]
    assert f"Retry 1 for layer roads: {clean}" in caplog.messages
    assert [message for message in caplog.messages if "SECRET" in message] == []


@pytest.mark.integration
def test_extracted_catalog_has_a_producer_and_a_thumbnail(
    fixtures_dir: Path, tmp_path: Path
) -> None:
    """#1039: the run names the producer and draws the thumbnail (PTL-PRV-001, PTL-VIZ-001)."""
    import geoparquet_io as gpio

    from portolan_cli.metadata_extraction import ExtractedMetadata

    points = fixtures_dir / "vector" / "valid" / "points.geojson"

    class _AttributedSource(_FakeSource):
        def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
            seed = ExtractedMetadata(
                source_type="fake", source_url=source, attribution="City of Philadelphia"
            )
            return Discovery(source_url=source, layers=self.layers, catalog_seed=seed)

        def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
            table = gpio.convert(str(points))
            table.write(str(dest))
            return int(table.num_rows)

    out = tmp_path / "out"
    run_layer_extraction(
        _AttributedSource(_refs(["points"])),
        "https://h.example/FeatureServer",
        out,
        _options(raw=False, license="CC0-1.0", catalog_id="phl-points"),
    )

    assert json.loads((out / "catalog.json").read_text(encoding="utf-8"))["id"] == "phl-points"
    collection = json.loads((out / "points" / "collection.json").read_text(encoding="utf-8"))
    assert collection["providers"] == [{"name": "City of Philadelphia", "roles": ["producer"]}]
    thumbnails = [key for key, a in collection["assets"].items() if "thumbnail" in a["roles"]]
    assert thumbnails == ["thumbnail"]
