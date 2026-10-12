"""Shared pytest fixtures for Portolan CLI tests."""

from __future__ import annotations

# Set matplotlib backend to Agg BEFORE any matplotlib imports.
# Required for headless CI environments (Windows/Linux without display).
# Conditional: matplotlib is in the [thumbnails] extra, not needed for all tests.
try:
    import matplotlib as mpl

    mpl.use("Agg")
except ModuleNotFoundError:
    pass  # Iceberg tests don't need matplotlib

import os
import sys
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from tests.network_guard import NO_PROXY_KEYS, PROXY_KEYS, NetworkGuard, SentinelProxy

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


# =============================================================================
# Environment Isolation
# =============================================================================

# mutmut instruments every mutated function with a trampoline that reads
# os.environ["MUTANT_UNDER_TEST"] on each call into portolan_cli. A bare
# patch.dict(os.environ, {}, clear=True) wipes that var, so any cleared-env test
# that then calls into portolan_cli fails the mutation baseline with
# KeyError: 'MUTANT_UNDER_TEST' (portolan-sdi/portolan-cli#612). Preserve just
# that one bookkeeping var. Outside mutmut it is absent, so this is identical to
# a full clear.
_MUTMUT_ENV_KEY = "MUTANT_UNDER_TEST"

# botocore reads ~/.aws itself, so these point it at an empty directory. A test
# that clears the environment must keep them, or the developer's real profiles
# reach the code under test. It must also keep the IMDS switch, or botocore
# asks 169.254.169.254 for instance credentials.
_AWS_ISOLATION_KEYS = (
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_EC2_METADATA_DISABLED",
)

# mutmut also re-invokes Hypothesis tests through its own runner, once per
# mutant in the same process, so class-based property tests see more than one
# executor instance and trip HealthCheck.differing_executors during the
# mutation baseline (#612). Register a profile that relaxes exactly that check
# when running under mutmut; a test's own @settings(suppress_health_check=...)
# replaces this list, so explicit lists in test files carry the entry
# themselves. Outside mutmut the default profile stays fully strict.
if _MUTMUT_ENV_KEY in os.environ:
    from hypothesis import HealthCheck as _HealthCheck
    from hypothesis import settings as _hypothesis_settings

    _hypothesis_settings.register_profile(
        "mutmut",
        suppress_health_check=[_HealthCheck.differing_executors],
    )
    _hypothesis_settings.load_profile("mutmut")

    # Under mutmut the whole suite runs in one long-lived process that pytest
    # did not start, so the root logger arrives carrying whatever handlers and
    # level that process left on it. Library records (rasterio and GDAL are the
    # loud ones) then reach the stream CliRunner captures, and every test that
    # does json.loads(result.output) dies on "Extra data" past the envelope.
    # That is how the mutation gate failed on #612. Reset the root logger to the
    # state a plain pytest run starts from: no handlers, level WARNING. pytest
    # adds its own capture handlers per test phase, so caplog is unaffected.
    import logging as _logging

    _root_logger = _logging.getLogger()
    for _handler in list(_root_logger.handlers):
        _root_logger.removeHandler(_handler)
    _root_logger.setLevel(_logging.WARNING)


# CI selects a reduced, deterministic Hypothesis profile with
# HYPOTHESIS_PROFILE=ci (set in .github/workflows/ci.yml). A PR run gets a fast
# reproducible check. Nightly does not set the variable, so it keeps the
# default profile with 100 randomized examples, and example exploration is not
# lost. A test's own @settings(...) overrides these values.
if _MUTMUT_ENV_KEY not in os.environ and os.environ.get("HYPOTHESIS_PROFILE") == "ci":
    from hypothesis import settings as _ci_hypothesis_settings

    _ci_hypothesis_settings.register_profile(
        "ci",
        max_examples=25,
        derandomize=True,
        print_blob=True,
    )
    _ci_hypothesis_settings.load_profile("ci")


@contextmanager
def aws_files_environ(aws_dir: Path) -> Iterator[None]:
    """Point the AWS readers at ``aws_dir`` instead of ~/.aws.

    ``_aws_shared_file`` and botocore both read these variables, so a fixture
    that writes profiles to a temporary directory must name them here.
    """
    with mock.patch.dict(
        os.environ,
        {
            "AWS_SHARED_CREDENTIALS_FILE": str(aws_dir / "credentials"),
            "AWS_CONFIG_FILE": str(aws_dir / "config"),
        },
    ):
        yield


@contextmanager
def cleared_environ(**overrides: str) -> Iterator[None]:
    """Clear os.environ for the block, preserving the harness bookkeeping vars.

    Drop-in replacement for ``mock.patch.dict(os.environ, {}, clear=True)`` that
    keeps the environment sandbox-stable under mutation testing. It also keeps
    the AWS isolation vars, so botocore cannot read the developer's real
    profiles. It keeps the proxy vars of the network guard, so obstore and GDAL
    cannot reach a remote host. Pass keyword ``overrides`` to seed specific
    variables into the otherwise-empty environment.
    """
    keep = (_MUTMUT_ENV_KEY, *_AWS_ISOLATION_KEYS, *PROXY_KEYS, *NO_PROXY_KEYS)
    preserved = {k: v for k, v in os.environ.items() if k in keep}
    preserved.update(overrides)
    with mock.patch.dict(os.environ, preserved, clear=True):
        yield


# geoparquet-io installs DuckDB extensions on first use. The network guard
# refuses that download, so a fresh CI runner could load none of them. Install
# them once, before any test starts. Under xdist only the controller does it,
# before it starts the workers. An installed extension costs no download.
# Each entry is (name, repository). None is the core repository. geoparquet-io
# loads h3, geography (s2), and a5 from the community repository.
_DUCKDB_EXTENSIONS: tuple[tuple[str, str | None], ...] = (
    ("spatial", None),
    ("httpfs", None),
    ("encodings", None),
    ("h3", "community"),
    ("geography", "community"),
    ("a5", "community"),
)


def pytest_configure(config: pytest.Config) -> None:
    """Install the DuckDB extensions before the network guard starts."""
    if hasattr(config, "workerinput"):
        return
    import duckdb

    connection = duckdb.connect()
    try:
        for name, repository in _DUCKDB_EXTENSIONS:
            try:
                connection.install_extension(name, repository=repository)
            except duckdb.Error as exc:
                # Offline, or no build for this platform. A test that needs the
                # extension then fails and names extensions.duckdb.org.
                print(f"Could not install the DuckDB {name} extension: {exc}", file=sys.stderr)
    finally:
        connection.close()


# mutmut drives pytest in-process and runs the suite several times per
# invocation: a stats pass, a clean pass, a forced-fail pass, then one pass per
# mutant. Hypothesis remembers the instance that ran a @given method and fails
# HealthCheck.differing_executors the second time that method runs against a
# fresh instance, so the clean pass dies on the first class-based property test
# it reaches and takes the night's sweep with it (portolan-sdi/portolan-cli#612).
# Those 100-odd tests are ordinary pytest methods, not the reused executors the
# check is aimed at.
#
# Dropping the example database answers what the check actually warns about:
# a failing example recorded while one mutant was live would replay against the
# next, crediting a kill to the wrong mutant. Outside mutmut the var is absent
# and the check stays on.
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Keep class-based Hypothesis tests alive across mutmut's repeated sessions."""
    if _MUTMUT_ENV_KEY not in os.environ:
        return

    from hypothesis import HealthCheck, settings

    for item in items:
        obj = getattr(item, "obj", None)
        func = getattr(obj, "__func__", obj)
        if not hasattr(func, "hypothesis"):
            continue
        current = getattr(func, "_hypothesis_internal_use_settings", None) or settings.default
        if HealthCheck.differing_executors in current.suppress_health_check:
            continue
        func._hypothesis_internal_use_settings = settings(
            parent=current,
            suppress_health_check=[
                *current.suppress_health_check,
                HealthCheck.differing_executors,
            ],
            database=None,
        )


# =============================================================================
# Fixture Directory Access
# =============================================================================


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


# =============================================================================
# Vector Fixtures (GeoJSON, GeoParquet)
# =============================================================================


@pytest.fixture(scope="session")
def valid_points_geojson(fixtures_dir: Path) -> Path:
    """Path to valid points GeoJSON fixture (10 point features)."""
    return fixtures_dir / "vector" / "valid" / "points.geojson"


@pytest.fixture(scope="session")
def valid_polygons_geojson(fixtures_dir: Path) -> Path:
    """Path to valid polygons GeoJSON fixture (5 polygon features)."""
    return fixtures_dir / "vector" / "valid" / "polygons.geojson"


@pytest.fixture(scope="session")
def valid_lines_geojson(fixtures_dir: Path) -> Path:
    """Path to valid lines GeoJSON fixture (5 linestring features)."""
    return fixtures_dir / "vector" / "valid" / "lines.geojson"


@pytest.fixture(scope="session")
def valid_multigeom_geojson(fixtures_dir: Path) -> Path:
    """Path to valid mixed geometry GeoJSON fixture."""
    return fixtures_dir / "vector" / "valid" / "multigeom.geojson"


@pytest.fixture(scope="session")
def valid_large_properties_geojson(fixtures_dir: Path) -> Path:
    """Path to valid GeoJSON with 20+ property columns."""
    return fixtures_dir / "vector" / "valid" / "large_properties.geojson"


@pytest.fixture(scope="session")
def valid_points_parquet(fixtures_dir: Path) -> Path:
    """Path to valid GeoParquet fixture (real-world Open Buildings data).

    This uses real-world data from Open Buildings (1000 building footprints).
    Previously pointed to synthetic fixture at vector/valid/points.parquet.
    """
    return fixtures_dir / "realdata" / "open-buildings.parquet"


@pytest.fixture(scope="session")
def projected_parquet(fixtures_dir: Path) -> Path:
    """Path to GeoParquet with projected CRS (EPSG:32631 UTM Zone 31N)."""
    return fixtures_dir / "vector" / "open-buildings-utm31n.parquet"


# Invalid vector fixtures


@pytest.fixture(scope="session")
def invalid_no_geometry_json(fixtures_dir: Path) -> Path:
    """Path to JSON file with no geometry field."""
    return fixtures_dir / "vector" / "invalid" / "no_geometry.json"


@pytest.fixture(scope="session")
def invalid_malformed_geojson(fixtures_dir: Path) -> Path:
    """Path to malformed (truncated) GeoJSON file."""
    return fixtures_dir / "vector" / "invalid" / "malformed.geojson"


@pytest.fixture(scope="session")
def invalid_empty_geojson(fixtures_dir: Path) -> Path:
    """Path to empty FeatureCollection GeoJSON file."""
    return fixtures_dir / "vector" / "invalid" / "empty.geojson"


@pytest.fixture(scope="session")
def invalid_null_geometries_geojson(fixtures_dir: Path) -> Path:
    """Path to GeoJSON with null geometry features."""
    return fixtures_dir / "vector" / "invalid" / "null_geometries.geojson"


# =============================================================================
# Raster Fixtures (COG)
# =============================================================================


@pytest.fixture(scope="session")
def valid_rgb_cog(fixtures_dir: Path) -> Path:
    """Path to valid COG fixture (real-world RapidAI4EO satellite imagery).

    This uses real-world data from ESA's RapidAI4EO program (100x100 Sentinel-2 sample).
    Previously pointed to synthetic fixture at raster/valid/rgb.tif.
    """
    return fixtures_dir / "realdata" / "rapidai4eo-sample.tif"


@pytest.fixture(scope="session")
def valid_singleband_cog(fixtures_dir: Path) -> Path:
    """Path to valid single-band COG fixture (64x64)."""
    return fixtures_dir / "raster" / "valid" / "singleband.tif"


@pytest.fixture(scope="session")
def valid_float32_cog(fixtures_dir: Path) -> Path:
    """Path to valid float32 COG fixture (elevation-like data)."""
    return fixtures_dir / "raster" / "valid" / "float32.tif"


@pytest.fixture(scope="session")
def valid_nodata_cog(fixtures_dir: Path) -> Path:
    """Path to valid COG with nodata value set."""
    return fixtures_dir / "raster" / "valid" / "nodata.tif"


# Invalid raster fixtures


@pytest.fixture(scope="session")
def invalid_not_georeferenced_tif(fixtures_dir: Path) -> Path:
    """Path to TIFF without CRS or geotransform."""
    return fixtures_dir / "raster" / "invalid" / "not_georeferenced.tif"


@pytest.fixture(scope="session")
def invalid_truncated_tif(fixtures_dir: Path) -> Path:
    """Path to truncated (corrupted) TIFF file."""
    return fixtures_dir / "raster" / "invalid" / "truncated.tif"


@pytest.fixture(scope="session")
def non_cog_tif(fixtures_dir: Path) -> Path:
    """Path to non-cloud-optimized GeoTIFF (Natural Earth data)."""
    return fixtures_dir / "raster" / "natural-earth-non-cog.tif"


# =============================================================================
# Edge Case Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def edge_unicode_geojson(fixtures_dir: Path) -> Path:
    """Path to GeoJSON with Unicode property values."""
    return fixtures_dir / "edge" / "unicode_properties.geojson"


@pytest.fixture(scope="session")
def edge_special_filename_geojson(fixtures_dir: Path) -> Path:
    """Path to GeoJSON with spaces in filename."""
    return fixtures_dir / "edge" / "special_filename spaces.geojson"


@pytest.fixture(scope="session")
def edge_antimeridian_geojson(fixtures_dir: Path) -> Path:
    """Path to GeoJSON crossing the antimeridian."""
    return fixtures_dir / "edge" / "antimeridian.geojson"


# =============================================================================
# Real-World Data Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def nwi_wetlands_path(fixtures_dir: Path) -> Path:
    """NWI Wetlands - complex polygons with holes (1,000 features)."""
    return fixtures_dir / "realdata" / "nwi-wetlands.parquet"


@pytest.fixture(scope="session")
def open_buildings_path(fixtures_dir: Path) -> Path:
    """Open Buildings - bulk polygon ingestion (1,000 features)."""
    return fixtures_dir / "realdata" / "open-buildings.parquet"


@pytest.fixture(scope="session")
def road_detections_path(fixtures_dir: Path) -> Path:
    """Road Detections - LineString geometries (1,000 features)."""
    return fixtures_dir / "realdata" / "road-detections.parquet"


@pytest.fixture(scope="session")
def fieldmaps_boundaries_path(fixtures_dir: Path) -> Path:
    """FieldMaps Boundaries - antimeridian crossing (3 features)."""
    return fixtures_dir / "realdata" / "fieldmaps-boundaries.parquet"


@pytest.fixture(scope="session")
def rapidai4eo_path(fixtures_dir: Path) -> Path:
    """RapidAI4EO - Cloud-Optimized GeoTIFF raster."""
    return fixtures_dir / "realdata" / "rapidai4eo-sample.tif"


# =============================================================================
# Temporary Catalog Fixtures
# =============================================================================


@pytest.fixture
def temp_catalog_dir(tmp_path: Path) -> Iterator[Path]:
    """Create a temporary directory for catalog operations.

    Yields the path to a clean temporary directory that will be
    automatically cleaned up after the test.
    """
    catalog_dir = tmp_path / "test-catalog"
    catalog_dir.mkdir()
    yield catalog_dir


# =============================================================================
# Catalog State Fixtures (for push/pull/sync tests)
# =============================================================================


@pytest.fixture
def catalog_with_versions_for_dry_run(tmp_path: Path) -> Path:
    """Catalog with -compliant structure for dry-run tests.

    This fixture is used by TestDryRunNetworkIsolation classes across
    test_pull.py, test_push.py, and test_sync.py.

    Structure:
    - <catalog_root>/catalog.json
    - <catalog_root>/.portolan/config.yaml
    (Note: state.json removed per issue #290 - config.yaml alone is sufficient)
    - <catalog_root>/<collection>/versions.json
    - <catalog_root>/<collection>/data.parquet
    """
    import json

    catalog_dir = tmp_path / "catalog_dry_run"
    catalog_dir.mkdir()

    # Create catalog.json at root
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog for dry-run tests",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # .portolan sentinel: config.yaml alone is sufficient for MANAGED state (per issue #290)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("{}\n")
    (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')

    # Versions.json at <catalog_root>/<collection>/versions.json
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1000,
                        "href": "test-collection/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))
    (collection_dir / "data.parquet").write_bytes(b"x" * 1000)

    return catalog_dir


@pytest.fixture
def fresh_catalog_no_versions(tmp_path: Path) -> Path:
    """Catalog WITHOUT versions.json (fresh state for initial pull/push).

    This tests the edge case where dry-run is called on a catalog that
    has never been versioned before.
    """
    import json

    catalog_dir = tmp_path / "catalog_fresh"
    catalog_dir.mkdir()

    # Create catalog.json at root
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Fresh catalog without versions",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # .portolan sentinel: config.yaml alone is sufficient (per issue #290)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("{}\n")
    (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')

    # Collection directory exists but NO versions.json
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    return catalog_dir


@pytest.fixture
def catalog_with_multiple_versions(tmp_path: Path) -> Path:
    """Catalog with 3 versions for divergence and accumulation testing.

    Version history:
    - v1.0.0: base.parquet (initial)
    - v1.1.0: base.parquet + second.parquet (add)
    - v1.2.0: base.parquet + second.parquet + third.parquet (add)

    Useful for testing snapshot model accumulation.
    Uses real SHA256 hashes computed from file content.
    """
    import hashlib
    import json

    catalog_dir = tmp_path / "catalog_multi_version"
    catalog_dir.mkdir()

    # .portolan sentinel
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("catalog_id: test-catalog\n")
    (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')

    # catalog.json
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Catalog with multiple versions",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Collection with 3 versions
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    # Create actual asset files first, then compute real hashes
    base_content = b"x" * 1000
    second_content = b"y" * 2000
    third_content = b"z" * 3000

    (collection_dir / "base.parquet").write_bytes(base_content)
    (collection_dir / "second.parquet").write_bytes(second_content)
    (collection_dir / "third.parquet").write_bytes(third_content)

    # Compute real SHA256 hashes from content
    base_hash = hashlib.sha256(base_content).hexdigest()
    second_hash = hashlib.sha256(second_content).hexdigest()
    third_hash = hashlib.sha256(third_content).hexdigest()

    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.2.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2026-01-15T10:00:00Z",
                "breaking": False,
                "assets": {
                    "base.parquet": {
                        "sha256": base_hash,
                        "size_bytes": 1000,
                        "href": "base.parquet",
                    }
                },
                "changes": ["base.parquet"],
            },
            {
                "version": "1.1.0",
                "created": "2026-01-16T10:00:00Z",
                "breaking": False,
                "assets": {
                    "base.parquet": {
                        "sha256": base_hash,
                        "size_bytes": 1000,
                        "href": "base.parquet",
                    },
                    "second.parquet": {
                        "sha256": second_hash,
                        "size_bytes": 2000,
                        "href": "second.parquet",
                    },
                },
                "changes": ["second.parquet"],
            },
            {
                "version": "1.2.0",
                "created": "2026-01-17T10:00:00Z",
                "breaking": False,
                "assets": {
                    "base.parquet": {
                        "sha256": base_hash,
                        "size_bytes": 1000,
                        "href": "base.parquet",
                    },
                    "second.parquet": {
                        "sha256": second_hash,
                        "size_bytes": 2000,
                        "href": "second.parquet",
                    },
                    "third.parquet": {
                        "sha256": third_hash,
                        "size_bytes": 3000,
                        "href": "third.parquet",
                    },
                },
                "changes": ["third.parquet"],
            },
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    return catalog_dir


# =============================================================================
# Conformant catalogs (rashid)
# =============================================================================

# `portolan check` validates against rashid's PTL-* rule set, so a hand-built
# catalog.json is no longer a usable fixture: it trips PTL-CNF-001 (no schema
# URI), PTL-FIL-001 (no README/AGENTS.md), PTL-TTL-001 (no title), and more,
# and every test asserting on check output drowns in unrelated findings.
#
# `build_conformant_catalog` builds the catalog the way `portolan init` does:
# through the real generator, so it stays conformant by construction as
# generation changes (the same property the conformance gate in
# tests/integration/test_generated_catalog_conformance.py locks down).


def _init_catalog(
    root: Path,
    *,
    title: str = "Test Catalog",
    description: str = "A catalog built for the test suite.",
) -> Path:
    """Run `portolan init` into ``root`` and return it."""
    from click.testing import CliRunner

    from portolan_cli.cli import cli

    root.mkdir(parents=True, exist_ok=True)
    result = CliRunner().invoke(
        cli,
        [
            "init",
            str(root),
            "--auto",
            "--title",
            title,
            "--description",
            description,
            "--license",
            "CC-BY-4.0",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    return root


@pytest.fixture
def build_conformant_catalog() -> Callable[..., Path]:
    """Factory building a catalog that passes rashid's metadata pass.

    Use this wherever a test needs a catalog to exist without being the subject
    of the test. Findings the test asserts on should be ones it introduces
    itself, by editing the generated tree.
    """
    return _init_catalog


@pytest.fixture
def conformant_catalog(tmp_path: Path) -> Path:
    """A generated, conformant catalog at ``tmp_path/catalog``."""
    return _init_catalog(tmp_path / "catalog")


@pytest.fixture(autouse=True)
def isolate_aws_config(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Point botocore at an empty AWS config for every test.

    ``_resolve_credential_provider`` builds a botocore session. botocore reads
    ~/.aws itself, so patching ``Path.home`` does not hide the developer's real
    profiles from it. Without this fixture a machine with credentials and a
    machine without them run different tests.

    The fixture also clears the provider cache, because that cache keys on the
    profile name alone and would otherwise cross a test boundary.
    """
    from portolan_cli.sync.upload import _boto3_credential_provider

    empty = tmp_path_factory.mktemp("aws-isolation")
    _boto3_credential_provider.cache_clear()
    with mock.patch.dict(
        os.environ,
        {
            "AWS_CONFIG_FILE": str(empty / "config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(empty / "credentials"),
            # botocore probes 169.254.169.254 when no profile supplies keys.
            # The probe blocks for 2 seconds wherever that address drops the
            # packet, once for every store the suite builds.
            "AWS_EC2_METADATA_DISABLED": "true",
        },
    ):
        yield


@pytest.fixture(scope="session")
def _sentinel_proxy() -> Iterator[SentinelProxy]:
    yield from SentinelProxy().run()


@pytest.fixture(autouse=True)
def network_guard(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    _sentinel_proxy: SentinelProxy,
) -> Iterator[NetworkGuard]:
    """Fail a test without the network marker that connects to a remote host.

    ``tests/network_guard.py`` describes the two layers. A test that expects a
    block can read ``collect()`` and clear ``attempts``.
    """
    guard = NetworkGuard(test=request.node.nodeid, sentinel=_sentinel_proxy)
    if request.node.get_closest_marker("network") is not None:
        yield guard
        return
    guard.install(monkeypatch)
    yield guard
    marker = request.node.get_closest_marker("refused_hosts")
    unexpected = guard.unexpected(marker.args if marker is not None else ())
    if unexpected:
        counts = Counter(unexpected)
        targets = ", ".join(f"{t} ({n}x)" if n > 1 else t for t, n in counts.items())
        pytest.fail(
            f"{guard.test} tried to reach the network: {targets}. "
            "Mock the call, or mark the test with @pytest.mark.network.",
            pytrace=False,
        )


@pytest.fixture(autouse=True)
def no_basemap_tiles(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Draw thumbnails without basemap tiles in a test without the network marker.

    ``add_basemap`` fetches the tiles from the provider and logs a failure as a
    warning, so about 170 tests fetched Carto tiles on each run without notice.
    A test of the basemap code patches ``_ensure_contextily`` itself, and its
    patch replaces this one.
    """
    if request.node.get_closest_marker("network") is not None:
        return
    try:
        from portolan_cli.viz import thumbnail
    except ImportError:
        return  # the environment has no thumbnail extra, so no test draws one
    monkeypatch.setattr(thumbnail, "_ensure_contextily", lambda: None)


@pytest.fixture
def no_extract_side_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the requests the extract orchestrators make next to the feature requests.

    Each request logs a failure and goes on, so a test that mocks only the
    feature requests still reaches the remote host:

    - ``extract arcgis`` fetches the ESRI layer JSON for ``drawingInfo``.
    - ``extract arcgis`` auto-init fetches the layer details for a description.
    - ``extract wfs`` calls WMS GetStyles and GetLegendGraphic.
    - ``extract wfs`` negotiates the WFS version with GetCapabilities.

    Each stub returns the result that a failed request gives: no style, no
    layer details, and WFS 1.1.0. A test of one of these requests patches it
    itself, and its patch replaces this one.
    """
    from geoparquet_io.core import wfs as gpio_wfs  # type: ignore[import-untyped]

    from portolan_cli.extract.arcgis import discovery
    from portolan_cli.extract.arcgis import orchestrator as arcgis
    from portolan_cli.extract.wfs import orchestrator as wfs

    monkeypatch.setattr(arcgis, "extract_esri_style", lambda *a, **k: None)
    # ArcGISLayerSource.layer_metadata imports it inside the method.
    monkeypatch.setattr(discovery, "fetch_layer_details", lambda *a, **k: {})
    monkeypatch.setattr(wfs, "extract_wms_style", lambda *a, **k: None)
    monkeypatch.setattr(wfs, "extract_wms_legend", lambda *a, **k: None)
    # _negotiate_version imports it inside the function.
    monkeypatch.setattr(gpio_wfs, "negotiate_wfs_version", lambda *a, **k: ("1.1.0", None))


@pytest.fixture(autouse=True)
def isolate_installed_plugins(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    """Load no outside plugin, except in tests marked ``plugin`` (#947).

    The plugin docs install the example plugin into the project environment.
    It then stays installed, and its hooks write fields into every STAC file
    that `add` produces. Without this fixture a developer machine and CI run
    different tests. The ``installed`` and ``register_plugin`` fixtures clear
    the variable again for the tests that control the plugin set.
    """
    from portolan_cli.plugins import manager

    if request.node.get_closest_marker("plugin") is None:
        monkeypatch.setenv(manager.ENV_VAR, "none")
    manager.reset_plugin_manager()
    yield
    manager.reset_plugin_manager()


@pytest.fixture
def register_plugin(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[..., None]]:
    """Register plugin objects on a fresh plugin manager (#947).

    The manager sees no installed plugin, so only built-ins and the objects a
    test registers take part.
    """
    from portolan_cli.plugins import manager

    monkeypatch.setattr(manager, "entry_points", lambda *, group: [])
    monkeypatch.delenv(manager.ENV_VAR, raising=False)
    manager.reset_plugin_manager()

    def _register(plugin: object, name: str = "test-plugin") -> None:
        manager.get_plugin_manager().register(plugin, name=name)

    yield _register
    manager.reset_plugin_manager()


@pytest.fixture(autouse=True)
def no_remote_key_listing(request: pytest.FixtureRequest) -> Iterator[None]:
    """Stop a test from listing a real bucket before a push (Issue #965).

    Push lists the remote keys before it uploads, so that a rollback keeps the
    keys that existed. Many tests push to ``s3://mybucket`` with mocked
    uploads. The listing then goes to the network and blocks for seconds.

    A remote store gets None here. That means "state unknown", so a rollback
    deletes nothing and sends no DELETE either. Three cases run the real
    listing. An in-process store, such as MemoryStore, runs it. A test that
    uses the ``moto_server`` fixture runs it against moto. A test with the
    ``network`` marker runs it against the real store.
    """
    if "moto_server" in request.fixturenames or request.node.get_closest_marker("network"):
        yield
        return

    from obstore.store import LocalStore, MemoryStore

    from portolan_cli.sync import push as push_module

    real = push_module._existing_keys_before_push

    async def in_process_only(
        store: object, prefix: str, collection: str, planned_keys: list[str]
    ) -> set[str] | None:
        """Run the real key read for an in-process store, else report the state unknown."""
        if isinstance(store, MemoryStore | LocalStore):
            return await real(store, prefix, collection, planned_keys)
        return None

    with mock.patch.object(push_module, "_existing_keys_before_push", in_process_only):
        yield
