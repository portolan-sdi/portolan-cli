"""The portolan-core writer on a catalog with an alternate-language tree."""

from __future__ import annotations

import copy
import json
from typing import TYPE_CHECKING, Any

import pytest
from rashid_core.api import ValidationOptions, validate

from portolan_core.writer import LocalSink, PutOperation, apply_plan, plan_catalog
from tests.support.core_catalog import golden_documentation, golden_documents

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration

LANGUAGE = "https://stac-extensions.github.io/language/v1.0.0/schema.json"
ROMANIAN = {"code": "ro", "name": "Română", "alternate": "Romanian"}
ENGLISH = {"code": "en", "name": "English"}


def _alternate(href: str, language: dict[str, str]) -> dict[str, str]:
    return {
        "rel": "alternate",
        "href": href,
        "type": "application/json",
        "title": language["name"],
        "hreflang": language["code"],
    }


def _translated(document: dict[str, Any], own: dict[str, str], other: dict[str, str]) -> None:
    if LANGUAGE not in document["stac_extensions"]:
        document["stac_extensions"].append(LANGUAGE)
    document["language"] = own
    document["languages"] = [other]


def _documents() -> dict[str, dict[str, Any]]:
    golden = golden_documents()
    root = golden["catalog.json"]
    root["links"] = [_alternate("./en/catalog.json", ENGLISH)]
    _translated(root, ROMANIAN, ENGLISH)
    boundaries = golden["boundaries/collection.json"]
    boundaries["links"] = [_alternate("../en/boundaries/collection.json", ENGLISH)]
    _translated(boundaries, ROMANIAN, ENGLISH)

    en_root = copy.deepcopy(root)
    en_root["links"] = [_alternate("../catalog.json", ROMANIAN)]
    _translated(en_root, ENGLISH, ROMANIAN)
    en_boundaries = copy.deepcopy(boundaries)
    en_boundaries["links"] = [_alternate("../../boundaries/collection.json", ROMANIAN)]
    _translated(en_boundaries, ENGLISH, ROMANIAN)
    for asset in en_boundaries["assets"].values():
        asset["href"] = asset["href"].replace("./", "../../boundaries/", 1)
    return {
        "catalog.json": root,
        "boundaries/collection.json": boundaries,
        "en/catalog.json": en_root,
        "en/boundaries/collection.json": en_boundaries,
    }


def _documentation() -> dict[str, str]:
    golden = golden_documentation()
    pairs = {key: golden[key] for key in ("README.md", "AGENTS.md")}
    pairs |= {
        f"boundaries/{name}": golden[f"boundaries/{name}"] for name in ("README.md", "AGENTS.md")
    }
    return pairs | {f"en/{key}": text for key, text in pairs.items()}


def _structural(content: bytes) -> list[tuple[str, str]]:
    links = json.loads(content)["links"]
    return [(link["rel"], link["href"]) for link in links]


def test_each_language_tree_gets_its_own_root_and_no_cross_tree_child() -> None:
    plan = plan_catalog(_documents(), _documentation())
    puts = {op.key: op.content for op in plan.operations if isinstance(op, PutOperation)}

    assert _structural(puts["catalog.json"]) == [
        ("root", "./catalog.json"),
        ("child", "./boundaries/collection.json"),
        ("agents", "./AGENTS.md"),
        ("describedby", "./README.md"),
        ("alternate", "./en/catalog.json"),
    ]
    assert _structural(puts["en/catalog.json"]) == [
        ("root", "./catalog.json"),
        ("child", "./boundaries/collection.json"),
        ("agents", "./AGENTS.md"),
        ("describedby", "./README.md"),
        ("alternate", "../catalog.json"),
    ]
    assert _structural(puts["en/boundaries/collection.json"]) == [
        ("root", "../catalog.json"),
        ("parent", "../catalog.json"),
        ("agents", "./AGENTS.md"),
        ("describedby", "./README.md"),
        ("alternate", "../../boundaries/collection.json"),
    ]


def test_the_validator_accepts_the_translated_catalog(tmp_path: Path) -> None:
    apply_plan(plan_catalog(_documents(), _documentation()), LocalSink(tmp_path))

    report = validate(tmp_path, options=ValidationOptions(data=False))

    assert [(f.rule_id, f.path, f.message) for f in report.errors] == []
