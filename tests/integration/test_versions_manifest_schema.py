"""Compatibility guard for the CLI-owned ``versions.json`` manifest.

``versions.json`` is the CLI's single source of truth for version history,
checksums, and sync state. It is CLI-internal, not part of the published
Portolan specification, so rashid says nothing about it — the schemas in
``tests/fixtures/versions-manifest/`` and these tests are its only guard. A
catalog written by an older CLI must stay readable by a newer one; a failure
here is a compatibility break, not a style problem.

There are TWO different versions.json structures:

- Catalog-level (root): ``schema_version``, ``catalog_id``, ``created``,
  ``collections`` — described by ``catalog-versions.schema.json``.
- Collection-level: ``spec_version``, ``current_version``, ``versions`` —
  described by ``versions.schema.json``.

The semantic invariants a JSON Schema cannot express (current-version
consistency, change references, version uniqueness) are validated by the
helpers below, and the negative tests keep both directions non-tautological.
The format is documented in ``docs/reference/versions-manifest.md``.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

pytestmark = pytest.mark.integration

_SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "versions-manifest"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(scope="session")
def versions_schema() -> dict[str, Any]:
    """Collection-level versions.json schema (version history)."""
    result: dict[str, Any] = json.loads(
        (_SCHEMAS_DIR / "versions.schema.json").read_text(encoding="utf-8")
    )
    return result


@pytest.fixture(scope="session")
def catalog_versions_schema() -> dict[str, Any]:
    """Root-level versions.json schema (collection index)."""
    result: dict[str, Any] = json.loads(
        (_SCHEMAS_DIR / "catalog-versions.schema.json").read_text(encoding="utf-8")
    )
    return result


def _validate(data: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    import jsonschema

    validator = jsonschema.Draft202012Validator(schema)
    return [f"{e.json_path}: {e.message}" for e in validator.iter_errors(data)]


# =============================================================================
# Semantic invariants a JSON Schema cannot express
# =============================================================================


def _check_current_version_consistency(versions_data: dict[str, Any]) -> list[str]:
    """current_version must match the last entry in the versions array."""
    errors: list[str] = []
    current = versions_data.get("current_version")
    versions = versions_data.get("versions", [])

    if current is not None:
        if not versions:
            errors.append("current_version is set but versions array is empty")
        elif current != versions[-1].get("version"):
            errors.append(
                f"current_version '{current}' does not match "
                f"last version '{versions[-1].get('version')}'"
            )
    elif versions:
        errors.append("current_version is null but versions array is not empty")

    return errors


def _check_changes_reference_assets(versions_data: dict[str, Any]) -> list[str]:
    """Every changes entry must name a key that exists in that version's assets."""
    errors: list[str] = []
    for i, version in enumerate(versions_data.get("versions", [])):
        assets = set(version.get("assets", {}).keys())
        for change in version.get("changes", []):
            if change not in assets:
                errors.append(f"version[{i}].changes references '{change}' which is not in assets")
    return errors


def _check_version_uniqueness(versions_data: dict[str, Any]) -> list[str]:
    """Version strings must be unique within the versions array."""
    version_strings = [v.get("version") for v in versions_data.get("versions", [])]
    duplicates = {v for v in version_strings if version_strings.count(v) > 1}
    if duplicates:
        return [f"duplicate version strings: {duplicates}"]
    return []


def _add_points_collection(runner: CliRunner, fixture: Path, name: str = "points") -> Path:
    """Init a catalog and add ``fixture`` under a new collection directory."""
    result = runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])
    assert result.exit_code == 0, f"init failed: {result.output}"

    collection_dir = Path(name)
    collection_dir.mkdir()
    target = collection_dir / fixture.name
    shutil.copy(fixture, target)

    result = runner.invoke(cli, ["add", str(target)])
    assert result.exit_code == 0, f"add failed: {result.output}"
    return collection_dir


# =============================================================================
# CLI output conforms to the schemas
# =============================================================================


class TestCatalogVersionsSchemaCompliance:
    """Root-level versions.json complies with catalog-versions.schema.json."""

    def test_init_creates_valid_catalog_versions_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        catalog_versions_schema: dict[str, Any],
    ) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            data = json.loads(Path("versions.json").read_text())
            errors = _validate(data, catalog_versions_schema)
            assert not errors, "Schema validation failed:\n" + "\n".join(errors)

    def test_add_keeps_catalog_versions_json_valid(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        catalog_versions_schema: dict[str, Any],
    ) -> None:
        """Regression for #561: ``update_catalog_versions`` wrote keys the
        schema rejected; ``init`` alone never exercised this write path."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _add_points_collection(runner, valid_points_geojson)

            data = json.loads(Path("versions.json").read_text())
            errors = _validate(data, catalog_versions_schema)
            assert not errors, "Schema validation failed:\n" + "\n".join(errors)


class TestCollectionVersionsSchemaCompliance:
    """Collection-level versions.json complies with versions.schema.json."""

    def test_add_creates_valid_versions_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        versions_schema: dict[str, Any],
    ) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            collection_dir = _add_points_collection(runner, valid_points_geojson)

            data = json.loads((collection_dir / "versions.json").read_text())
            errors = _validate(data, versions_schema)
            assert not errors, "Schema validation failed:\n" + "\n".join(errors)

    def test_add_geoparquet_creates_valid_versions_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_parquet: Path,
        versions_schema: dict[str, Any],
    ) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            collection_dir = _add_points_collection(runner, valid_points_parquet, name="buildings")

            data = json.loads((collection_dir / "versions.json").read_text())
            errors = _validate(data, versions_schema)
            assert not errors, "Schema validation failed:\n" + "\n".join(errors)


class TestVersionsSemanticInvariants:
    """CLI output satisfies the invariants a JSON Schema cannot express."""

    def test_written_manifest_satisfies_all_invariants(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            collection_dir = _add_points_collection(runner, valid_points_geojson)
            data = json.loads((collection_dir / "versions.json").read_text())

            errors = (
                _check_current_version_consistency(data)
                + _check_changes_reference_assets(data)
                + _check_version_uniqueness(data)
            )
            assert not errors, "Invariant validation failed:\n" + "\n".join(errors)


# =============================================================================
# Negative tests: the guard itself rejects invalid data (never tautological)
# =============================================================================


class TestSchemaRejectsInvalid:
    """The schemas reject malformed manifests."""

    def test_rejects_missing_required(self, versions_schema: dict[str, Any]) -> None:
        errors = _validate({"spec_version": "1.0.0"}, versions_schema)
        assert errors, "schema accepted a manifest missing current_version/versions"

    def test_rejects_invalid_semver(self, versions_schema: dict[str, Any]) -> None:
        invalid = {"spec_version": "not-a-semver", "current_version": "1.0.0", "versions": []}
        errors = _validate(invalid, versions_schema)
        assert errors, "schema accepted a non-semver spec_version"

    def test_rejects_non_utc_timestamp(self, versions_schema: dict[str, Any]) -> None:
        invalid = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-15T10:30:00+05:00",  # not UTC (missing Z)
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                }
            ],
        }
        errors = _validate(invalid, versions_schema)
        assert errors, "schema accepted a non-UTC timestamp"

    def test_rejects_invalid_sha256(self, versions_schema: dict[str, Any]) -> None:
        invalid = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-15T10:30:00Z",
                    "breaking": False,
                    "assets": {
                        "data.parquet": {
                            "sha256": "not-a-valid-sha256",
                            "size_bytes": 1000,
                            "href": "collection/data.parquet",
                        }
                    },
                    "changes": ["data.parquet"],
                }
            ],
        }
        errors = _validate(invalid, versions_schema)
        assert errors, "schema accepted an invalid sha256"


class TestInvariantCheckersRejectInvalid:
    """The semantic checkers reject violations."""

    def test_rejects_mismatched_current_version(self) -> None:
        invalid = {
            "current_version": "1.0.0",
            "versions": [{"version": "1.0.0"}, {"version": "2.0.0"}],
        }
        assert _check_current_version_consistency(invalid)

    def test_rejects_null_current_with_versions(self) -> None:
        invalid = {"current_version": None, "versions": [{"version": "1.0.0"}]}
        assert _check_current_version_consistency(invalid)

    def test_rejects_phantom_change_reference(self) -> None:
        invalid = {
            "versions": [{"assets": {"real-file.parquet": {}}, "changes": ["non-existent.parquet"]}]
        }
        assert _check_changes_reference_assets(invalid)

    def test_rejects_duplicate_versions(self) -> None:
        invalid = {"versions": [{"version": "1.0.0"}, {"version": "1.0.0"}]}
        assert _check_version_uniqueness(invalid)
