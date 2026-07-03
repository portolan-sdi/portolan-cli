"""Spec compliance tests for the Portolan specification version (issue #566).

The specification carries a machine-readable SemVer version whose canonical home
is ``spec/schema/spec-version.json``. Everything else that names the version --
the CLI constant, ``rules.yaml``, and the schema ``$comment``s -- must mirror it,
so a spec change that forgets to bump one place fails here.

See ``spec/README.md#versioning`` for the bump policy.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from portolan_cli.constants import PORTOLAN_SPEC_VERSION

pytestmark = pytest.mark.unit

# Strict SemVer (major.minor.patch), no pre-release/build suffix for the spec.
_SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")

# Schemas that should advertise their spec version in a $comment.
_VERSIONED_SCHEMAS = (
    "versions.schema.json",
    "catalog.schema.json",
    "collection.schema.json",
    "catalog-versions.schema.json",
)


@pytest.fixture(scope="session")
def spec_version_doc(schemas_dir: Path) -> dict[str, Any]:
    """Load the canonical spec-version.json document."""
    result: dict[str, Any] = json.loads((schemas_dir / "spec-version.json").read_text())
    return result


def test_spec_version_file_exists(schemas_dir: Path) -> None:
    """The canonical machine-readable home exists under spec/schema/."""
    assert (schemas_dir / "spec-version.json").is_file()


def test_spec_version_is_valid_semver(spec_version_doc: dict[str, Any]) -> None:
    """spec_version is a strict SemVer string."""
    version = spec_version_doc["spec_version"]
    assert _SEMVER.match(version), f"{version!r} is not strict SemVer"


def test_spec_version_is_pre_1_0(spec_version_doc: dict[str, Any]) -> None:
    """The spec starts pre-1.0 (breaking changes bump minor until 1.0)."""
    major = int(spec_version_doc["spec_version"].split(".")[0])
    assert major == 0, "spec is pre-1.0; a 1.0 bump is an intentional policy change"


def test_constant_mirrors_canonical_version(spec_version_doc: dict[str, Any]) -> None:
    """The CLI constant matches the canonical spec-version.json (no drift)."""
    assert PORTOLAN_SPEC_VERSION == spec_version_doc["spec_version"]


def test_rules_yaml_matches_canonical_version(
    schemas_dir: Path, spec_version_doc: dict[str, Any]
) -> None:
    """rules.yaml advertises the same spec version as the canonical file."""
    rules = yaml.safe_load((schemas_dir / "rules.yaml").read_text())
    assert rules["spec_version"] == spec_version_doc["spec_version"]


@pytest.mark.parametrize("schema_name", _VERSIONED_SCHEMAS)
def test_schema_comment_names_spec_version(
    schemas_dir: Path, spec_version_doc: dict[str, Any], schema_name: str
) -> None:
    """Each JSON schema's $comment references the current spec version."""
    schema = json.loads((schemas_dir / schema_name).read_text())
    assert spec_version_doc["spec_version"] in schema["$comment"]
