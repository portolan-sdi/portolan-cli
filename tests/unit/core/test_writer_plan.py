"""Tests for the catalog writer plan in portolan_core.writer (#1133)."""

from __future__ import annotations

import copy
import dataclasses
import json
from typing import Any

import pytest
from rashid_core.api import newest_profile_version, profile_uri

from portolan_core.writer import (
    DeleteOperation,
    InvalidDocumentError,
    InvalidKeyError,
    MissingOwnerError,
    PutOperation,
    WritePlan,
    apply_plan,
    plan_catalog,
)
from tests.support.core_catalog import (
    SOUTH,
    golden_documentation,
    golden_documents,
    golden_files,
)

pytestmark = pytest.mark.unit


def _plan(**kwargs: Any) -> WritePlan:
    return plan_catalog(golden_documents(), golden_documentation(), **kwargs)


def _puts(plan: WritePlan) -> dict[str, PutOperation]:
    return {op.key: op for op in plan.operations if isinstance(op, PutOperation)}


def _deletes(plan: WritePlan) -> list[str]:
    return [op.key for op in plan.operations if isinstance(op, DeleteOperation)]


class TestGoldenCatalog:
    def test_puts_match_the_hand_written_catalog_byte_for_byte(self) -> None:
        puts = _puts(_plan())

        assert sorted(puts) == sorted(golden_files())
        for key, expected in golden_files().items():
            assert puts[key].content == expected, key

    def test_media_types_follow_the_file_kind(self) -> None:
        puts = _puts(_plan())

        assert puts["catalog.json"].media_type == "application/json"
        assert puts["boundaries/collection.json"].media_type == "application/json"
        assert puts[f"{SOUTH}/Estación Sur.json"].media_type == "application/geo+json"
        assert puts["README.md"].media_type == "text/markdown"

    def test_each_object_is_written_after_every_object_it_links_down_to(self) -> None:
        keys = [op.key for op in _plan().operations]

        assert keys[-1] == "catalog.json"
        assert keys.index(f"{SOUTH}/Estación Sur.json") < keys.index(
            "environment/air-quality/collection.json"
        )
        assert keys.index("environment/air-quality/collection.json") < keys.index(
            "environment/catalog.json"
        )
        assert keys.index("boundaries/README.md") < keys.index("boundaries/collection.json")

    def test_the_input_documents_are_not_changed(self) -> None:
        documents = golden_documents()
        before = copy.deepcopy(documents)

        plan_catalog(documents, golden_documentation())

        assert documents == before

    def test_a_missing_profile_uri_gets_the_newest_bundled_profile(self) -> None:
        documents = golden_documents()
        del documents["environment/catalog.json"]["stac_extensions"]

        content = _puts(plan_catalog(documents, golden_documentation()))[
            "environment/catalog.json"
        ].content

        assert json.loads(content)["stac_extensions"] == [profile_uri(newest_profile_version())]


class TestManagedDeletes:
    PRIOR = (
        "catalog.json",
        "removed/collection.json",
        "removed/README.md",
        "removed/AGENTS.md",
        "removed/item-1/item-1.json",
    )

    def test_a_removed_collection_listed_as_managed_is_deleted(self) -> None:
        plan = _plan(prior_managed_keys=self.PRIOR)

        assert _deletes(plan) == [
            "removed/item-1/item-1.json",
            "removed/AGENTS.md",
            "removed/README.md",
            "removed/collection.json",
        ]

    def test_deletes_come_after_every_put(self) -> None:
        operations = _plan(prior_managed_keys=self.PRIOR).operations

        kinds = [isinstance(op, PutOperation) for op in operations]
        assert kinds == sorted(kinds, reverse=True)

    def test_keys_never_listed_are_not_deleted(self) -> None:
        assert _deletes(_plan()) == []

    def test_the_key_prefix_applies_to_puts_and_deletes(self) -> None:
        plan = _plan(prior_managed_keys=self.PRIOR, key_prefix="catalogs/demo/")

        assert sorted(_puts(plan)) == sorted(f"catalogs/demo/{k}" for k in golden_files())
        assert _deletes(plan) == [
            "catalogs/demo/removed/item-1/item-1.json",
            "catalogs/demo/removed/AGENTS.md",
            "catalogs/demo/removed/README.md",
            "catalogs/demo/removed/collection.json",
        ]

    def test_a_prior_key_that_is_not_catalog_metadata_is_refused(self) -> None:
        with pytest.raises(InvalidKeyError, match="boundaries/boundaries.parquet"):
            _plan(prior_managed_keys=["boundaries/boundaries.parquet"])


class TestPlainData:
    def test_the_same_input_gives_the_same_operations(self) -> None:
        assert _plan(prior_managed_keys=["old/collection.json"]) == _plan(
            prior_managed_keys=["old/collection.json"]
        )

    def test_the_plan_round_trips_through_plain_dicts(self) -> None:
        plan = _plan(prior_managed_keys=["old/collection.json"])

        data = dataclasses.asdict(plan)
        rebuilt = WritePlan(
            operations=tuple(
                PutOperation(**op) if "content" in op else DeleteOperation(**op)
                for op in data["operations"]
            )
        )

        assert rebuilt == plan
        assert data["operations"][-1] == {"key": "old/collection.json"}


class TestDirectoryAsset:
    def test_a_directory_asset_passes_through_without_a_checksum(self) -> None:
        documents = golden_documents()
        zarr = {
            "href": "./pm25.zarr/",
            "type": "application/vnd+zarr",
            "roles": ["data"],
            "file:size": 123456,
        }
        documents[f"{SOUTH}/Estación Sur.json"]["assets"]["pm25"] = zarr

        content = _puts(plan_catalog(documents, golden_documentation()))[
            f"{SOUTH}/Estación Sur.json"
        ].content

        assert json.loads(content)["assets"]["pm25"] == zarr


class TestBadInput:
    @pytest.mark.parametrize("key", ["/abs/catalog.json", "a/../catalog.json", "", "a//b.json"])
    def test_a_bad_document_key_is_refused(self, key: str) -> None:
        documents = golden_documents()
        documents[key] = documents["environment/catalog.json"]

        with pytest.raises(InvalidKeyError):
            plan_catalog(documents, golden_documentation())

    @pytest.mark.parametrize("prefix", ["/abs", "a/../b"])
    def test_a_bad_key_prefix_is_refused(self, prefix: str) -> None:
        with pytest.raises(InvalidKeyError):
            _plan(key_prefix=prefix)

    def test_a_child_without_a_title_is_refused(self) -> None:
        documents = golden_documents()
        del documents["boundaries/collection.json"]["title"]

        with pytest.raises(InvalidDocumentError, match="boundaries/collection.json"):
            plan_catalog(documents, golden_documentation())

    def test_a_catalog_without_its_readme_is_refused(self) -> None:
        documentation = golden_documentation()
        del documentation["environment/README.md"]

        with pytest.raises(InvalidDocumentError, match="environment/README.md"):
            plan_catalog(golden_documents(), documentation)

    def test_documentation_without_an_owner_is_refused(self) -> None:
        documentation = golden_documentation()
        documentation["ghost/README.md"] = "# Ghost\n"

        with pytest.raises(MissingOwnerError, match="ghost/README.md"):
            plan_catalog(golden_documents(), documentation)

    def test_a_document_of_the_wrong_kind_is_refused(self) -> None:
        documents = golden_documents()
        documents["environment/catalog.json"]["type"] = "Collection"

        with pytest.raises(InvalidDocumentError, match="environment/catalog.json"):
            plan_catalog(documents, golden_documentation())

    def test_stac_extensions_that_is_not_a_list_is_refused(self) -> None:
        documents = golden_documents()
        documents["environment/catalog.json"]["stac_extensions"] = "not-a-list"

        with pytest.raises(InvalidDocumentError, match="environment/catalog.json"):
            plan_catalog(documents, golden_documentation())

    def test_a_tree_without_a_root_catalog_is_refused(self) -> None:
        documents = golden_documents()
        del documents["catalog.json"]
        documentation = golden_documentation()
        del documentation["README.md"], documentation["AGENTS.md"]

        with pytest.raises(InvalidDocumentError, match="catalog.json"):
            plan_catalog(documents, documentation)

    def test_an_item_outside_a_collection_is_refused(self) -> None:
        documents = golden_documents()
        documents["environment/stray/stray.json"] = documents.pop(f"{SOUTH}/Estación Sur.json")

        with pytest.raises(InvalidDocumentError, match="environment/stray/stray.json"):
            plan_catalog(documents, golden_documentation())


class TestCustomSink:
    def test_a_custom_sink_receives_the_operations_in_plan_order(self) -> None:
        calls: list[tuple[str, str]] = []

        class RecordingSink:
            def put(self, key: str, content: bytes, media_type: str) -> None:
                calls.append(("put", key))

            def delete(self, key: str) -> None:
                calls.append(("delete", key))

        plan = _plan(prior_managed_keys=["old/collection.json"])
        apply_plan(plan, RecordingSink())

        assert calls == [
            ("put" if isinstance(op, PutOperation) else "delete", op.key) for op in plan.operations
        ]
        assert calls[-1] == ("delete", "old/collection.json")


def _links_of(plan: WritePlan, key: str) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = json.loads(_puts(plan)[key].content)["links"]
    return links


class TestReviewFixes:
    def test_keys_that_differ_only_by_case_are_refused(self) -> None:
        item = "environment/air-quality/station-north/station-north.json"

        with pytest.raises(InvalidKeyError, match="Station-North.json"):
            _plan(prior_managed_keys=[item.replace("station-north.json", "Station-North.json")])

    def test_a_prior_key_that_a_new_asset_references_is_refused(self) -> None:
        documents = golden_documents()
        documents["boundaries/collection.json"]["assets"]["style"] = {
            "href": "./style.json",
            "type": "application/json",
            "roles": ["style"],
        }

        with pytest.raises(InvalidKeyError, match="boundaries/style.json"):
            plan_catalog(
                documents, golden_documentation(), prior_managed_keys=["boundaries/style.json"]
            )

    @pytest.mark.parametrize("key", ["versions.json", "boundaries/versions.json"])
    def test_a_versions_json_prior_key_is_refused(self, key: str) -> None:
        with pytest.raises(InvalidKeyError, match="versions.json"):
            _plan(prior_managed_keys=[key])

    def test_absolute_structural_links_stay_in_place(self) -> None:
        documents = golden_documents()
        external = {
            "rel": "child",
            "href": "https://other.example/collection.json",
            "type": "application/json",
            "title": "Elsewhere",
        }
        via = documents["boundaries/collection.json"]["links"][1]
        documents["boundaries/collection.json"]["links"] = [external, via]

        links = _links_of(
            plan_catalog(documents, golden_documentation()), "boundaries/collection.json"
        )

        assert links[-2:] == [external, via]
        assert [link["rel"] for link in links] == [
            "root",
            "parent",
            "agents",
            "describedby",
            "child",
            "via",
        ]

    def test_a_describedby_link_to_another_markdown_file_stays(self) -> None:
        documents = golden_documents()
        dictionary = {"rel": "describedby", "href": "./data-dictionary.md", "type": "text/markdown"}
        documents["boundaries/collection.json"]["links"].append(dictionary)

        links = _links_of(
            plan_catalog(documents, golden_documentation()), "boundaries/collection.json"
        )

        assert links[-1] == dictionary
        assert [link["href"] for link in links if link["rel"] == "describedby"] == [
            "./README.md",
            "./data-dictionary.md",
        ]

    def test_a_nested_collection_is_refused(self) -> None:
        documents = golden_documents()
        documents["boundaries/inner/collection.json"] = copy.deepcopy(
            documents["boundaries/collection.json"]
        )
        documentation = golden_documentation()
        documentation["boundaries/inner/README.md"] = "# Inner\n"
        documentation["boundaries/inner/AGENTS.md"] = "# Inner\n"

        with pytest.raises(InvalidDocumentError, match="boundaries/inner/collection.json"):
            plan_catalog(documents, documentation)

    @pytest.mark.parametrize("prefix", ["/", "///", "a//b", "a\\b", "a/./b"])
    def test_a_prefix_with_an_empty_or_bad_segment_is_refused(self, prefix: str) -> None:
        with pytest.raises(InvalidKeyError):
            _plan(key_prefix=prefix)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("links", {"rel": "child"}),
            ("links", ["not-a-link"]),
            ("links", [{"rel": [], "href": "x"}]),
            ("links", [{"rel": "via", "href": 3}]),
            ("assets", ["not-a-mapping"]),
            ("assets", {"data": "not-an-asset"}),
            ("assets", {"data": {"href": None}}),
        ],
    )
    def test_malformed_links_or_assets_are_refused(self, field: str, value: object) -> None:
        documents = golden_documents()
        documents["boundaries/collection.json"][field] = value

        with pytest.raises(InvalidDocumentError, match="boundaries/collection.json"):
            plan_catalog(documents, golden_documentation())
