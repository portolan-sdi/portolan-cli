"""Spec compliance: validate CLI output against the SHIPPED schemas.

Unlike the older tests that validated against hand-copied inline schema stubs
(which only covered Portolan extensions, never the STAC base), these tests load
the real ``spec/schema/catalog.schema.json`` and ``spec/schema/collection.schema.json``
and validate CLI-generated output against them.

The shipped schemas ``allOf``-reference the upstream STAC v1.1.0 draft-07
schemas by URL. Those references are resolved from a vendored copy bundled with
``portolan_cli.validation`` (see ``schema_registry``), so validation is
hermetic: it never touches the network.

Href/IRI format is intentionally NOT enforced here. Portolan emits relative
hrefs by design and the href policy is unresolved (see discussion #573), so the
validator runs with JSON Schema ``format`` assertions off. These tests check
structural conformance only.

See issue #557.
"""

from __future__ import annotations

import json
import shutil
import socket
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from referencing.exceptions import Unresolvable

from portolan_cli.cli import cli
from portolan_cli.validation.schema_registry import build_stac_registry, validate_document

# STAC base schema URIs the shipped Portolan schemas reference. If the vendored
# closure fails to register these, ``$ref`` resolution would fall back to the
# network (or fail), so this is the canary for "the bundle is complete".
_STAC_CATALOG_URI = "https://schemas.stacspec.org/v1.1.0/catalog-spec/json-schema/catalog.json"
_STAC_COLLECTION_URI = (
    "https://schemas.stacspec.org/v1.1.0/collection-spec/json-schema/collection.json"
)


# ``catalog_schema`` and ``collection_schema`` (the shipped schemas loaded from
# disk) come from tests/spec_compliance/conftest.py.


class TestVendoredClosureComplete:
    """The vendored STAC closure must cover every reference the shipped schemas make."""

    @pytest.mark.unit
    def test_registry_resolves_stac_catalog_base_offline(self) -> None:
        registry = build_stac_registry()
        # crawl=False resolution: the resource is present without a fetch.
        resource = registry.get_or_retrieve(_STAC_CATALOG_URI).value
        assert resource.contents.get("$id", "").endswith("catalog.json")

    @pytest.mark.unit
    def test_registry_resolves_stac_collection_base_offline(self) -> None:
        registry = build_stac_registry()
        resource = registry.get_or_retrieve(_STAC_COLLECTION_URI).value
        assert "collection" in json.dumps(resource.contents).lower()


class TestShippedSchemaValidatesRealOutput:
    """CLI-generated catalogs/collections conform to the SHIPPED schemas."""

    @pytest.mark.integration
    def test_init_catalog_conforms_to_shipped_catalog_schema(
        self,
        runner: CliRunner,
        tmp_path: Path,
        catalog_schema: dict[str, Any],
    ) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            data = json.loads(Path("catalog.json").read_text())
            errors = validate_document(data, catalog_schema)
            assert not errors, "shipped catalog schema rejected init output:\n" + "\n".join(errors)

    @pytest.mark.integration
    def test_add_collection_conforms_to_shipped_schemas(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        catalog_schema: dict[str, Any],
        collection_schema: dict[str, Any],
    ) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            assert runner.invoke(cli, ["init", "--auto"]).exit_code == 0

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            catalog = json.loads(Path("catalog.json").read_text())
            cat_errors = validate_document(catalog, catalog_schema)
            assert not cat_errors, "shipped catalog schema rejected add output:\n" + "\n".join(
                cat_errors
            )

            collection_path = collection_dir / "collection.json"
            assert collection_path.exists(), "collection.json not created"
            collection = json.loads(collection_path.read_text())
            col_errors = validate_document(collection, collection_schema)
            assert not col_errors, "shipped collection schema rejected add output:\n" + "\n".join(
                col_errors
            )


class TestShippedSchemaEnforcesConstraints:
    """A schema edit that changes what is valid must change what the suite accepts.

    These feed deliberately non-conformant documents through the SHIPPED schema
    and require errors. If someone loosens a shipped schema incompatibly, the
    corresponding assertion here flips, so the suite is a real guard (checkbox 4).
    """

    @pytest.mark.unit
    def test_catalog_missing_title_is_rejected(self, catalog_schema: dict[str, Any]) -> None:
        # Portolan extension requires human-readable title (RULE-0100).
        bad_catalog = {
            "type": "Catalog",
            "stac_version": "1.1.0",
            "id": "x",
            "description": "d",
            "links": [],
        }
        errors = validate_document(bad_catalog, catalog_schema)
        assert errors, "shipped catalog schema accepted a catalog with no title"

    @pytest.mark.unit
    def test_stac_base_required_fields_enforced(self, catalog_schema: dict[str, Any]) -> None:
        # stac_version/id/links are required by the STAC base schema (via $ref),
        # NOT by the Portolan extension. Catching them proves the vendored STAC
        # base is actually applied, not silently skipped -- the whole point of
        # loading the shipped schema instead of an extension-only stub (#557).
        base_only_violation = {"type": "Catalog", "title": "T", "description": "d"}
        errors = validate_document(base_only_violation, catalog_schema)
        joined = "\n".join(errors)
        assert "'stac_version' is a required property" in joined
        assert "'id' is a required property" in joined
        assert "'links' is a required property" in joined

    @pytest.mark.unit
    def test_child_link_without_title_is_rejected(self, catalog_schema: dict[str, Any]) -> None:
        # RULE-0102/0103: child/item links MUST carry a human-readable title.
        bad_catalog = {
            "type": "Catalog",
            "stac_version": "1.1.0",
            "id": "x",
            "title": "Title",
            "description": "d",
            "links": [{"rel": "child", "href": "./points/collection.json"}],
        }
        errors = validate_document(bad_catalog, catalog_schema)
        assert errors, "shipped catalog schema accepted a child link with no title"


class TestValidationIsHermetic:
    """Validation must resolve the STAC $refs offline and never fall back to the network."""

    @pytest.mark.unit
    def test_validation_works_with_sockets_blocked(
        self,
        monkeypatch: pytest.MonkeyPatch,
        catalog_schema: dict[str, Any],
    ) -> None:
        # Drop the cached registry so it is rebuilt from the vendored bundle
        # *under* the socket block. A warm lru_cache would otherwise let this
        # pass without exercising offline construction at all.
        build_stac_registry.cache_clear()

        def _no_network(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("validation attempted a network connection")

        monkeypatch.setattr(socket, "socket", _no_network)
        monkeypatch.setattr(socket, "create_connection", _no_network)

        good_catalog = {
            "type": "Catalog",
            "stac_version": "1.1.0",
            "id": "x",
            "title": "Title",
            "description": "d",
            "links": [],
        }
        # Resolves the STAC base $ref from the vendored bundle AND accepts a
        # conformant catalog -- with no network access. Asserting == [] (not just
        # "is a list") is what makes this a real conformance check.
        errors = validate_document(good_catalog, catalog_schema)
        assert errors == [], errors

    @pytest.mark.unit
    def test_unvendored_ref_raises_instead_of_fetching(self) -> None:
        # The registry is built with no retrieval callable, so a $ref outside the
        # vendored closure must raise Unresolvable rather than silently fetch it
        # over the network. This is what gives the socket-blocked test teeth:
        # an unresolved reference is a hard error, never a network round-trip.
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "allOf": [{"$ref": "https://schemas.stacspec.org/v1.1.0/not-vendored.json"}],
        }
        with pytest.raises(Unresolvable):
            validate_document({"type": "Catalog"}, schema)
