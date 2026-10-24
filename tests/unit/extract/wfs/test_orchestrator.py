"""Tests for WFS extraction orchestrator.

These tests verify the main orchestration logic for WFS extraction,
including layer filtering, extraction, and catalog initialization.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
import yaml

from portolan_cli.extract.common import source as source_module
from portolan_cli.extract.common.report import ExtractionReport, LayerResult
from portolan_cli.extract.common.source import (
    Discovery,
    LayerRef,
    LayerSourceOptions,
    build_report,
    seed_collections,
    slugify,
)
from portolan_cli.extract.wfs.discovery import LayerInfo
from portolan_cli.extract.wfs.orchestrator import (
    ExtractionOptions,
    WFSLayer,
    WFSLayerSource,
    _assign_slugs,
    extract_wfs_catalog,
)
from tests.support.extract import make_wfs_discovery, make_wfs_layer

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.extract.common.progress import ExtractionProgress

make_layer_info = make_wfs_layer
make_discovery_result = make_wfs_discovery

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_extract_side_requests")]


class TestExtractionOptions:
    """Tests for ExtractionOptions dataclass."""

    def test_default_values(self) -> None:
        """Default options have sensible values."""
        options = ExtractionOptions()
        assert options.workers == 1
        assert options.retries == 3
        assert options.timeout == 300.0  # 5 minutes per layer
        assert options.resume is False
        assert options.raw is False
        assert options.dry_run is False
        assert options.wfs_version == "auto"
        # Issue #529: adopt gpio 1.3+ defaults.
        assert options.page_size == 100000
        assert options.auto_tile is True


class TestExtractSingleLayer:
    """Tests for WFSLayerSource.fetch gpio delegation (Issue #529)."""

    @staticmethod
    def _fetch(options: ExtractionOptions, name: str, dest: Path) -> None:
        layer = make_layer_info(name, 0)
        ref = LayerRef(id=0, name=name, data=WFSLayer(layer, "2.0.0"))
        WFSLayerSource(options).fetch("https://example.com/wfs", ref, dest)

    def test_passes_page_size_and_auto_tile_to_gpio(self, tmp_path: Path) -> None:
        """page_size and auto_tile from options are forwarded to gpio."""
        options = ExtractionOptions(page_size=100000, auto_tile=True)

        with patch("geoparquet_io.core.wfs.convert_wfs_to_geoparquet") as mock_convert:
            self._fetch(options, "buildings", tmp_path / "buildings.parquet")

        mock_convert.assert_called_once()
        kwargs = mock_convert.call_args.kwargs
        assert kwargs["page_size"] == 100000
        assert kwargs["auto_tile"] is True
        assert kwargs["version"] == "2.0.0"

    def test_auto_tile_disabled_is_forwarded(self, tmp_path: Path) -> None:
        """auto_tile=False is forwarded so users can opt out."""
        options = ExtractionOptions(auto_tile=False)

        with patch("geoparquet_io.core.wfs.convert_wfs_to_geoparquet") as mock_convert:
            self._fetch(options, "roads", tmp_path / "roads.parquet")

        assert mock_convert.call_args.kwargs["auto_tile"] is False

    def test_empty_layer_error_becomes_empty_layer(self, tmp_path: Path) -> None:
        """gpio's 'No features returned' error becomes EmptyLayerError (Issue #450)."""
        from portolan_cli.extract.common.source import EmptyLayerError

        with (
            patch(
                "geoparquet_io.core.wfs.convert_wfs_to_geoparquet",
                side_effect=RuntimeError("No features returned from WFS service for layer 'x'."),
            ),
            pytest.raises(EmptyLayerError, match="No features returned"),
        ):
            self._fetch(ExtractionOptions(), "x", tmp_path / "x.parquet")


class TestExtractWfsCatalog:
    """Tests for extract_wfs_catalog function."""

    def test_dry_run_returns_pending_layers(self, tmp_path: Path) -> None:
        """Dry run returns report with pending status."""
        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("roads", 1),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        assert len(report.layers) == 2
        assert all(layer.status == "pending" for layer in report.layers)

    def test_dry_run_includes_service_metadata(self, tmp_path: Path) -> None:
        """Dry run report includes service metadata from discovery."""
        layers = [make_layer_info("buildings", 0)]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        # Service metadata should be in the report
        assert report.metadata_extracted.description == "A test WFS service"
        assert report.metadata_extracted.attribution == "Test Provider"
        assert "test" in (report.metadata_extracted.keywords or "")

    def test_layer_filter_applied(self, tmp_path: Path) -> None:
        """Layer filter reduces extracted layers."""
        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("roads", 1),
            make_layer_info("water", 2),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                layer_filter=["buildings"],
                options=options,
            )

        assert len(report.layers) == 1
        assert report.layers[0].name == "buildings"

    def test_layer_exclude_applied(self, tmp_path: Path) -> None:
        """Layer exclude removes matching layers."""
        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("test_layer", 1),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                layer_exclude=["test_*"],
                options=options,
            )

        assert len(report.layers) == 1
        assert report.layers[0].name == "buildings"


class TestVersionNegotiation:
    """Tests for WFS version negotiation."""

    def test_auto_version_negotiates(self, tmp_path: Path) -> None:
        """Auto version triggers negotiation."""
        layers = [make_layer_info("buildings", 0)]
        discovery = make_discovery_result(layers)

        with (
            patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.wfs.orchestrator._negotiate_version") as mock_negotiate,
        ):
            mock_discover.return_value = discovery
            mock_negotiate.return_value = "2.0.0"

            options = ExtractionOptions(dry_run=True, wfs_version="auto")
            extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        mock_negotiate.assert_called_once_with("https://example.com/wfs", "auto")
        # discover_layers should be called with the negotiated version
        mock_discover.assert_called_once()
        call_args = mock_discover.call_args
        assert call_args[1]["version"] == "2.0.0"

    def test_explicit_version_skips_negotiation(self, tmp_path: Path) -> None:
        """Explicit version skips negotiation."""
        layers = [make_layer_info("buildings", 0)]
        discovery = make_discovery_result(layers)

        with (
            patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.wfs.orchestrator._negotiate_version") as mock_negotiate,
        ):
            mock_discover.return_value = discovery
            mock_negotiate.return_value = "1.1.0"

            options = ExtractionOptions(dry_run=True, wfs_version="1.1.0")
            extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        # negotiate_version is called but returns the explicit version
        mock_negotiate.assert_called_once_with("https://example.com/wfs", "1.1.0")


class TestExtractWfsCatalogListMode:
    """Tests for list mode (no layer specified, no --all)."""

    def test_no_layer_filter_extracts_all(self, tmp_path: Path) -> None:
        """Without filter, extract all layers (matching ArcGIS behavior)."""
        layers = [make_layer_info("buildings", 0)]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        assert len(report.layers) == 1


class TestWFSMetadataSeeding:
    """Tests for WFS metadata seeding through the shared driver."""

    def test_report_metadata_to_wfs_metadata_conversion(self) -> None:
        """Converts MetadataExtracted to WFSMetadata correctly."""
        from portolan_cli.extract.common.report import MetadataExtracted
        from portolan_cli.extract.wfs.orchestrator import _report_metadata_to_wfs_metadata

        report_metadata = MetadataExtracted(
            source_url="https://example.com/wfs",
            description="A test WFS service",
            attribution="Test Provider",
            keywords=["test", "wfs"],
            contact_name="Test Contact",
            processing_notes="WFS service: Test Service",
            known_issues=None,
            license_info_raw="CC-BY-4.0",
        )

        wfs_metadata = _report_metadata_to_wfs_metadata(report_metadata)

        assert wfs_metadata.source_url == "https://example.com/wfs"
        assert wfs_metadata.service_abstract == "A test WFS service"
        assert wfs_metadata.provider_name == "Test Provider"
        assert wfs_metadata.keywords == ["test", "wfs"]
        assert wfs_metadata.access_constraints == "CC-BY-4.0"

    def test_discovery_seeds_catalog_metadata(self, tmp_path: Path) -> None:
        """The discovered service metadata seeds the catalog metadata.yaml."""
        from portolan_cli.extract.common.orchestrator_base import seed_catalog_metadata

        (tmp_path / ".portolan").mkdir()
        with (
            patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.wfs.orchestrator._negotiate_version", return_value="2.0.0"),
        ):
            mock_discover.return_value = make_discovery_result([make_layer_info("roads", 0)])
            found = WFSLayerSource().discover("https://example.com/wfs", LayerSourceOptions())

        seed_catalog_metadata(tmp_path, found.catalog_seed)

        content = (tmp_path / ".portolan" / "metadata.yaml").read_text()
        assert "Test Provider" in content

    @staticmethod
    def _report(name: str, status: str, output_path: str) -> ExtractionReport:
        return build_report(
            Discovery(source_url="https://example.com/wfs", layers=[]),
            [
                LayerResult(
                    id=0,
                    name=name,
                    status=status,
                    features=100 if status == "success" else None,
                    size_bytes=1000 if status == "success" else None,
                    duration_seconds=1.0 if status == "success" else None,
                    output_path=output_path,
                    warnings=[],
                    error=None if status == "success" else "Connection timeout",
                    attempts=1,
                )
            ],
        )

    @staticmethod
    def _seed(tmp_path: Path, info: LayerInfo, path: str, report: ExtractionReport) -> None:
        discovery = Discovery(
            source_url="https://example.com/wfs",
            layers=[LayerRef(id=info.id, name=info.name, path=path, data=WFSLayer(info, "2.0.0"))],
        )
        seed_collections(WFSLayerSource(), discovery, tmp_path, report)

    def test_seed_collection_metadata_wfs_creates_files(self, tmp_path: Path) -> None:
        """Collection-level metadata seeding creates per-collection metadata.yaml."""
        collection_dir = tmp_path / "buildings_abc123"
        (collection_dir / ".portolan").mkdir(parents=True)
        info = LayerInfo(
            name="ns:buildings",
            typename="ns:buildings",
            title="Buildings Layer",
            abstract="All buildings in the city",
            bbox=None,
            id=0,
        )

        self._seed(
            tmp_path,
            info,
            "buildings_abc123",
            self._report("ns:buildings", "success", "buildings_abc123/buildings_abc123.parquet"),
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        assert "All buildings in the city" in content
        assert "Buildings Layer" in content

    def test_seed_collection_metadata_wfs_skips_failed_layers(self, tmp_path: Path) -> None:
        """Collection seeding skips failed layer results."""
        self._seed(
            tmp_path,
            make_layer_info("failed_layer", 0),
            "failed_layer",
            self._report("failed_layer", "failed", ""),
        )

        assert not (tmp_path / "failed_layer" / ".portolan" / "metadata.yaml").exists()

    def test_seed_collection_metadata_wfs_uses_iso_metadata(self, tmp_path: Path) -> None:
        """ISO metadata from CSW takes precedence over WFS GetCapabilities metadata."""
        from portolan_cli.extract.csw.models import ISOMetadata

        collection_dir = tmp_path / "buildings_abc123"
        (collection_dir / ".portolan").mkdir(parents=True)
        info = LayerInfo(
            name="ns:buildings",
            typename="ns:buildings",
            title="WFS Title",
            abstract="WFS Abstract (should be overridden)",
            bbox=None,
            id=0,
            metadata_urls=[{"url": "https://csw.example.com/record"}],
        )
        mock_iso = ISOMetadata(
            file_identifier="test-iso-id",
            title="ISO Title",
            abstract="Rich ISO description from CSW",
            contact_organization="ISO Contact Org",
            contact_email="iso@example.com",
        )

        with patch(
            "portolan_cli.extract.wfs.orchestrator._try_fetch_iso_metadata",
            return_value=mock_iso,
        ):
            self._seed(
                tmp_path,
                info,
                "buildings_abc123",
                self._report(
                    "ns:buildings", "success", "buildings_abc123/buildings_abc123.parquet"
                ),
            )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        assert "Rich ISO description from CSW" in content
        assert "WFS Abstract" not in content
        # The ISO point of contact made the data. It does not maintain this copy.
        seeded = yaml.safe_load(content)
        assert seeded["providers"] == [{"name": "ISO Contact Org", "roles": ["producer"]}]
        assert "contact" not in seeded

    def test_seed_collection_metadata_wfs_nested_output_path(self, tmp_path: Path) -> None:
        """Collection seeding handles nested output paths correctly."""
        nested_dir = tmp_path / "service" / "layer_abc123"
        (nested_dir / ".portolan").mkdir(parents=True)
        info = LayerInfo(
            name="ns:layer",
            typename="ns:layer",
            title="Nested Layer",
            abstract="Layer in nested structure",
            bbox=None,
            id=0,
        )

        self._seed(
            tmp_path,
            info,
            "service/layer_abc123",
            self._report("ns:layer", "success", "service/layer_abc123/layer_abc123.parquet"),
        )

        content = (nested_dir / ".portolan" / "metadata.yaml").read_text()
        assert "Layer in nested structure" in content


class TestSlugify:
    """Tests for the shared slugify that WFS collection names use (Issue #379)."""

    def test_basic_slug(self) -> None:
        """Basic name is slugified without hash."""
        assert slugify("my_layer") == "my_layer"

    def test_slug_normalizes_special_chars(self) -> None:
        """Special characters converted to underscores."""
        assert slugify("ns:Feature-Type.Name") == "ns_feature_type_name"

    def test_slug_lowercase(self) -> None:
        """Names are lowercased."""
        assert slugify("MyLayer") == "mylayer"

    def test_slug_empty_name(self) -> None:
        """A name with no usable characters becomes 'unnamed'."""
        assert slugify("::") == "unnamed"


class TestAssignSlugs:
    """Tests for _assign_slugs function (Issue #379)."""

    def test_unique_names_no_hash(self) -> None:
        """Unique layer names get slugs without hash suffixes."""
        layers = [
            make_layer_info("layer_a", 0),
            make_layer_info("layer_b", 1),
            make_layer_info("layer_c", 2),
        ]
        slugs = _assign_slugs(layers)

        assert slugs[0] == "layer_a"
        assert slugs[1] == "layer_b"
        assert slugs[2] == "layer_c"

    def test_collision_gets_hash(self) -> None:
        """Colliding slugs get hash suffix."""
        # "ns:layer" and "ns_layer" both slugify to "ns_layer"
        layers = [
            make_layer_info("ns:layer", 0),
            make_layer_info("ns_layer", 1),
        ]
        slugs = _assign_slugs(layers)

        # Both should have hash suffixes due to collision
        assert "_" in slugs[0] and len(slugs[0].split("_")[-1]) == 6
        assert "_" in slugs[1] and len(slugs[1].split("_")[-1]) == 6
        # But they should be different
        assert slugs[0] != slugs[1]

    def test_partial_collision(self) -> None:
        """Only colliding names get hash, others stay clean."""
        # layer:a and layer_a both slugify to "layer_a" (collision)
        # layer_b and layer_c are unique
        layers = [
            make_layer_info("layer:a", 0),  # Slugifies to layer_a
            make_layer_info("layer_a", 1),  # Slugifies to layer_a (collision!)
            make_layer_info("layer_b", 2),  # Unique
            make_layer_info("layer_c", 3),  # Unique
        ]
        slugs = _assign_slugs(layers)

        # layer_b and layer_c: no collision, no hash
        assert slugs[2] == "layer_b"
        assert slugs[3] == "layer_c"

        # layer:a and layer_a: collision, both get hash
        assert len(slugs[0].split("_")[-1]) == 6
        assert len(slugs[1].split("_")[-1]) == 6
        assert slugs[0] != slugs[1]

    def test_identical_names_different_ids(self) -> None:
        """Identical names with different IDs produce different slugs."""
        # Edge case: exact same name but different layer IDs
        # (could happen with bad WFS data or duplicate entries)
        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("buildings", 1),
        ]
        slugs = _assign_slugs(layers)

        # Both should have hash suffixes
        assert len(slugs[0].split("_")[-1]) == 6
        assert len(slugs[1].split("_")[-1]) == 6
        # Critical: they must be DIFFERENT despite identical names
        assert slugs[0] != slugs[1]


class TestExtractionProgressError:
    """Tests for error field in ExtractionProgress (Issue #504)."""

    def test_progress_has_optional_error_field(self) -> None:
        """ExtractionProgress can carry an optional error message."""
        from portolan_cli.extract.common.progress import ExtractionProgress

        # Without error
        progress_ok = ExtractionProgress(
            layer_index=0,
            total_layers=1,
            layer_name="test_layer",
            status="success",
        )
        assert progress_ok.error is None

        # With error
        progress_fail = ExtractionProgress(
            layer_index=0,
            total_layers=1,
            layer_name="test_layer",
            status="failed",
            error="Connection refused",
        )
        assert progress_fail.error == "Connection refused"

    def test_on_progress_receives_error_on_failure(self, tmp_path: Path) -> None:
        """Progress callback receives error message when layer fails."""

        progress_events: list[ExtractionProgress] = []
        error_msg = "Unable to merge: Field has incompatible types"

        with (
            patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover,
            patch.object(WFSLayerSource, "fetch", side_effect=RuntimeError(error_msg)),
            patch("portolan_cli.extract.wfs.orchestrator._negotiate_version") as mock_version,
        ):
            mock_discover.return_value = make_discovery_result([make_layer_info("fail_layer", 0)])
            mock_version.return_value = "1.1.0"

            # A non-raw extraction builds a catalog, which needs a license (issue #686).
            options = ExtractionOptions(workers=1, retries=1, license="CC-BY-4.0")
            extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
                on_progress=progress_events.append,
            )

        failed_events = [p for p in progress_events if p.status == "failed"]
        assert len(failed_events) == 1
        assert failed_events[0].error == error_msg


class _VirtualClock:
    """Stand in for the ``time`` module of ``extract/common/source.py``.

    The clock moves only when a test advances it. A per-layer deadline then
    depends on the order of events in the run, not on the speed of the runner.
    """

    def __init__(self) -> None:
        self._now = 0.0
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += seconds


class TestTimeoutEnforcement:
    """Tests for per-layer timeout enforcement in a parallel run (Issue #508).

    The timeout tests use a virtual clock. A wall-clock version failed on
    loaded Windows runners, where the file I/O of one layer can stall for
    seconds.
    """

    @staticmethod
    def _run(
        tmp_path: Path,
        names: list[str],
        fetch: Callable[[str, LayerRef, Path], int],
        timeout: float,
        clock: _VirtualClock | None = None,
        on_progress: Callable[[ExtractionProgress], None] | None = None,
    ) -> ExtractionReport:
        with (
            patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.wfs.orchestrator._negotiate_version", return_value="1.1.0"),
            patch.object(WFSLayerSource, "fetch", side_effect=fetch),
            patch.object(source_module, "time", clock or time),
        ):
            mock_discover.return_value = make_discovery_result(
                [make_layer_info(name, i) for i, name in enumerate(names)]
            )
            return extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=ExtractionOptions(workers=2, timeout=timeout, retries=1, raw=True),
                on_progress=on_progress,
            )

    def test_slow_layer_times_out(self, tmp_path: Path) -> None:
        """A layer that exceeds the timeout is marked as timed out."""
        clock = _VirtualClock()
        slow_started = threading.Event()
        release = threading.Event()

        def fetch(source: str, layer: LayerRef, dest: Path) -> int:
            if layer.name == "slow_layer":
                slow_started.set()
                release.wait(timeout=30)
            return 1

        def on_progress(event: ExtractionProgress) -> None:
            # The fast layer is done, and the slow layer is in its fetch. Move
            # the clock past the deadline of the slow layer only.
            if event.layer_name == "fast_layer":
                assert slow_started.wait(timeout=30)
                clock.advance(5.0)

        try:
            report = self._run(
                tmp_path, ["slow_layer", "fast_layer"], fetch, 1.0, clock, on_progress
            )
        finally:
            release.set()

        slow, fast = report.layers
        assert (slow.status, slow.error) == ("failed", "Timeout after 1.0s")
        assert slow.duration_seconds == 5.0
        assert (fast.status, fast.error) == ("success", None)

    def test_fast_layers_complete_normally(self, tmp_path: Path) -> None:
        """Layers that complete within timeout succeed normally."""
        report = self._run(tmp_path, ["fast_a", "fast_b"], lambda *_: 100, timeout=60.0)

        assert [r.status for r in report.layers] == ["success", "success"]

    def test_queued_layers_do_not_time_out(self, tmp_path: Path) -> None:
        """Queued layers must not time out while waiting for a worker (Issue #526).

        The two workers meet at a barrier once in each fetch, and the clock
        moves 1s at each meeting. A running layer sees exactly 1s pass, far
        under the 10s timeout. 48 layers take 24 meetings, and the last layers
        wait 23s in the queue. A deadline from submission fails them.

        A fetch also takes a token, and the main thread gives one back for
        each layer it reports. The workers then stay at most one round ahead
        of the main thread. The main thread thus checks the deadlines while
        layers are still in the queue, and does not find all 48 done at once.

        The waits time out after 15s. One worker can stall for that long
        before the test fails.
        """
        clock = _VirtualClock()
        rounds = threading.Barrier(2, action=lambda: clock.advance(1.0))
        tokens = threading.Semaphore(2)

        def fetch(source: str, layer: LayerRef, dest: Path) -> int:
            if not tokens.acquire(timeout=15):
                raise TimeoutError("The main thread did not report a layer")
            rounds.wait(timeout=15)
            return 10

        try:
            report = self._run(
                tmp_path,
                [f"layer_{i}" for i in range(48)],
                fetch,
                10.0,
                clock,
                on_progress=lambda _: tokens.release(),
            )
        finally:
            # Free the workers that wait after a failed run.
            rounds.abort()
            tokens.release(48)

        assert [(r.status, r.error) for r in report.layers] == [("success", None)] * 48
        assert [r.duration_seconds for r in report.layers] == [1.0] * 48
        # The run lasted 24 virtual seconds, more than twice the timeout.
        assert clock.monotonic() == 24.0


def test_failed_version_negotiation_logs_no_key(caplog: pytest.LogCaptureFixture) -> None:
    """#981: geoparquet-io quotes the keyed URL. The warning names the service without the key."""
    from geoparquet_io.core.wfs import WFSError  # type: ignore[import-untyped]

    from portolan_cli.extract.wfs.orchestrator import _negotiate_version

    url = "https://h.example/geoserver/wfs?api_key=SECRET"
    failure = WFSError(f"Could not connect to WFS service: {url}")

    with patch("geoparquet_io.core.wfs.negotiate_wfs_version", side_effect=failure):
        version = _negotiate_version(url, "auto")

    assert version == "1.1.0"
    assert caplog.messages == [
        "Version negotiation failed, using 1.1.0: "
        "Could not connect to WFS service: https://h.example/geoserver/wfs"
    ]
