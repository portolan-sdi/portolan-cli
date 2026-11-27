"""Invariance properties of the portolan-core catalog writer.

``tests/unit/core/test_writer_plan.py`` and
``tests/integration/core/test_writer_apply.py`` pin the writer on one golden
catalog and on hand-picked bad keys. These properties generalize them over
generated catalogs and generated keys. Each property cites the sentence of
portolan-spec, or of a standard the spec builds on, that makes it a
requirement. The spec quotes come from portolan-spec at commit d48bae0.
"""

from __future__ import annotations

import copy
import json
import random
import tempfile
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from portolan_core import writer
from tests.integration.core.test_writer_apply import _tree

pytestmark = pytest.mark.unit

PROFILE = "https://schemas.portolan-sdi.org/portolan/v0.3.0/schema.json"
SLUG = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=8).map(
    lambda s: "c" + s
)
TITLE = st.text(
    alphabet=st.characters(categories=("L", "N", "Zs"), max_codepoint=0x24F),
    min_size=1,
    max_size=12,
).filter(str.strip)

OTHER_LINKS = st.builds(
    lambda rel, slug, title: {
        "rel": rel,
        "href": f"https://example.org/{slug}",
        "type": "text/html",
        "title": title,
    },
    st.sampled_from(["via", "license", "related"]),
    SLUG,
    TITLE,
)

Documents = dict[str, dict[str, Any]]
Input = tuple[Documents, dict[str, str]]


def _container(kind: str, node_id: str, title: str) -> dict[str, Any]:
    node: dict[str, Any] = {
        "type": kind,
        "stac_version": "1.1.0",
        "stac_extensions": [PROFILE],
        "id": node_id,
        "title": title,
        "description": f"The {title} {kind.lower()}.",
        "links": [],
    }
    if kind == "Collection":
        node["license"] = "CC-BY-4.0"
        node["extent"] = {
            "spatial": {"bbox": [[-5.0, 36.0, -4.0, 37.0]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        }
        node["assets"] = {"data": {"href": "./data.parquet", "roles": ["data"]}}
    return node


def _item(item_id: str, title: str | None) -> dict[str, Any]:
    properties: dict[str, Any] = {"datetime": "2024-06-01T00:00:00Z"}
    if title is not None:
        properties["title"] = title
    return {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": item_id,
        "geometry": {"type": "Point", "coordinates": [-4.5, 36.5]},
        "bbox": [-4.5, 36.5, -4.5, 36.5],
        "properties": properties,
        "assets": {"data": {"href": f"./{item_id}.tif", "roles": ["data"]}},
        "links": [],
    }


@st.composite
def catalogs(draw: st.DrawFn) -> Input:
    """A root catalog, an optional theme catalog, and 1 to 3 collections of 0 to 3 items."""
    documents: Documents = {"catalog.json": _container("Catalog", "root", draw(TITLE))}
    theme = draw(st.one_of(st.none(), SLUG))
    if theme is not None:
        documents[f"{theme}/catalog.json"] = _container("Catalog", theme, draw(TITLE))
    for name in draw(st.lists(SLUG, min_size=1, max_size=3, unique=True)):
        if name == theme:
            continue
        directory = f"{theme}/{name}" if theme and draw(st.booleans()) else name
        documents[f"{directory}/collection.json"] = _container("Collection", directory, draw(TITLE))
        for item_id in draw(st.lists(SLUG, max_size=3, unique=True)):
            title = draw(st.one_of(st.none(), TITLE))
            documents[f"{directory}/{item_id}/{item_id}.json"] = _item(item_id, title)
    # Links the planner does not make pass through in their input order.
    for node in documents.values():
        node["links"] = draw(st.lists(OTHER_LINKS, max_size=3))
    documentation = {
        f"{key.rsplit('/', 1)[0] + '/' if '/' in key else ''}{name}": f"# {name}\n"
        for key in documents
        if key.endswith(("catalog.json", "collection.json"))
        for name in ("README.md", "AGENTS.md")
    }
    return documents, documentation


def _shuffled(value: Any, rng: random.Random) -> Any:
    """``value`` with the keys of every JSON object in a random order."""
    if isinstance(value, dict):
        keys = list(value)
        rng.shuffle(keys)
        return {key: _shuffled(value[key], rng) for key in keys}
    if isinstance(value, list):
        return [_shuffled(entry, rng) for entry in value]
    return value


def _parsed(plan: writer.WritePlan) -> list[tuple[str, str, Any]]:
    """Each operation as (key, media type, parsed JSON or text)."""
    parsed: list[tuple[str, str, Any]] = []
    for op in plan.operations:
        assert isinstance(op, writer.PutOperation)
        body = op.content.decode()
        parsed.append(
            (op.key, op.media_type, json.loads(body) if op.key.endswith(".json") else body)
        )
    return parsed


@given(catalog=catalogs())
def test_equal_input_gives_an_equal_plan(catalog: Input) -> None:
    """best-practices/git-backed-catalogs.md, Keep the tools and inputs:
    "Generated output should be deterministic."

    Two equal inputs built apart, not one object planned twice, give equal plans.
    """
    documents, documentation = catalog
    assert writer.plan_catalog(documents, documentation) == writer.plan_catalog(
        copy.deepcopy(documents), dict(documentation)
    )


@given(catalog=catalogs(), seed=st.integers(min_value=0), indent=st.sampled_from([None, 0, 1, 4]))
def test_key_order_and_whitespace_in_the_input_do_not_change_the_catalog(
    catalog: Input, seed: int, indent: int | None
) -> None:
    """RFC 8259, section 4: "An object is an unordered collection of zero or
    more name/value pairs". Section 2: "Insignificant whitespace is allowed
    before or after any of the six structural characters." core.md, Core
    Structure, requires a valid STAC 1.1.0 catalog, and STAC objects are JSON.

    The input passes through JSON text with shuffled keys and other
    whitespace. Every written document is then the same JSON value, and the
    operations keep their keys, order, and media types. The bytes may differ,
    because the writer keeps the key order of its input on purpose.
    """
    documents, documentation = catalog
    text = json.dumps(_shuffled(documents, random.Random(seed)), indent=indent)
    reordered = json.loads(text)
    assert _parsed(writer.plan_catalog(reordered, documentation)) == _parsed(
        writer.plan_catalog(documents, documentation)
    )


@given(catalog=catalogs())
def test_apply_twice_changes_no_byte_and_a_replan_is_a_fixed_point(catalog: Input) -> None:
    """best-practices/git-backed-catalogs.md, Links and the publish step:
    "one tree of bytes, checked by CI on a laptop or a runner, published
    unchanged." And "TriMet's `tests/test_regen.py` rebuilds the catalog and
    compares it with the committed version".

    The sink holds exactly the planned bytes. A second apply leaves them
    unchanged. A plan made from the written tree is the plan that wrote it.
    """
    plan = writer.plan_catalog(*catalog)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        writer.apply_plan(plan, writer.LocalSink(root))
        first = _tree(root)
        writer.apply_plan(plan, writer.LocalSink(root))
        assert _tree(root) == first
    assert first == {
        op.key: op.content for op in plan.operations if isinstance(op, writer.PutOperation)
    }
    written = {key: json.loads(body) for key, body in first.items() if key.endswith(".json")}
    texts = {key: body.decode() for key, body in first.items() if key.endswith(".md")}
    assert writer.plan_catalog(written, texts) == plan


SEGMENT = st.text(alphabet="abcxyz0123456789_-", min_size=1, max_size=6)


@st.composite
def unsafe_keys(draw: st.DrawFn) -> str:
    """A key that is absolute, holds a parent segment, or starts with a drive."""
    parts = draw(st.lists(SEGMENT, min_size=1, max_size=3))
    leaf = draw(st.sampled_from(["catalog.json", "collection.json"]))
    shape = draw(st.sampled_from(["absolute", "parent", "drive"]))
    if shape == "absolute":
        return "/" + "/".join([*parts, leaf])
    if shape == "parent":
        parts.insert(draw(st.integers(min_value=0, max_value=len(parts))), "..")
        return "/".join([*parts, leaf])
    drive = draw(st.sampled_from("ABCDEXYZabcxyz"))
    return f"{drive}:" + draw(st.sampled_from(["/", ""])) + "/".join([*parts, leaf])


@given(
    catalog=catalogs(), key=unsafe_keys(), place=st.sampled_from(["document", "prefix", "prior"])
)
def test_an_unsafe_key_is_always_refused(catalog: Input, key: str, place: str) -> None:
    """core.md, Core Structure: "A Portolan catalog is a directory of STAC
    metadata and cloud-native geospatial data." Nested Catalogs, Flat
    Collections: "A nested collection's ID is its POSIX path from the catalog
    root".

    A key names a file inside that directory as a path from its root. An
    absolute key, a parent segment, or a drive leaves the directory. The
    planner refuses each one as a document key, a key prefix, or a prior key.
    """
    documents, documentation = catalog
    options: dict[str, Any] = {}
    if place == "document":
        documents[key] = _container("Catalog", "extra", "Extra")
    elif place == "prefix":
        options["key_prefix"] = key.rsplit("/", 1)[0]
    else:
        options["prior_managed_keys"] = [key]
    with pytest.raises(writer.InvalidKeyError):
        writer.plan_catalog(documents, documentation, **options)
