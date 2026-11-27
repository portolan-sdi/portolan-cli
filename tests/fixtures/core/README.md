# portolan-core writer fixtures

`golden_catalog/` is the expected output of `portolan_core.writer.plan_catalog`
for the input in `tests/support/core_catalog.py`. The files were written by
hand from the Portolan spec. Do not regenerate them with the writer or with
rashid's `expected_links()`. The writer and the validator share
`expected_links()`, so these files are the independent check against a bug
that both of them share.

The bad input that `tests/unit/core/test_writer_plan.py` builds is the invalid
variant of this fixture.

## Spec source of each expected link

The spec is `portolan-sdi/portolan-spec`, `specs/portolan/core.md`.

- **`root`.** Section "Links": "a catalog or collection MUST include `root`
  and `parent` links (except the root catalog, which has no parent)" and "an
  item MUST include `root`, `parent`, and `collection` links". So every file
  has a `root` link to `catalog.json`. The root catalog has a `root` link to
  itself (`./catalog.json`), as in `stac/examples/catalog.json`.
- **`parent`.** The same clause. The parent is the object one level up in the
  tree that section "Core Structure" draws. The root catalog has none.
  `boundaries/collection.json`, `environment/catalog.json`, and
  `environment/air-quality/collection.json` point at the catalog above them.
  Each item points at `environment/air-quality/collection.json`.
- **`collection`.** The same clause. Each item points at the collection that
  holds it.
- **`child` and `item`.** Section "Links": "a `child` or `item` link for every
  object it contains". The root holds `boundaries/` and `environment/`.
  `environment/` holds `air-quality/`. `air-quality/` holds the two items.
- **`type`.** Section "Links" requires a `type` on every structural link. The
  type is `application/geo+json` on a link to an item and `application/json`
  on the other links.
- **Titles.** Section "Human-Readable Titles": "every `child` and `item` link
  MUST include a `title`". A `child` link takes the `title` of its target.
  The link to `station-north` takes its `properties.title`. The
  item `Estación Sur` has no title. The link to it takes the item ID. That
  fallback is a writer choice, because the spec only requires that the title
  exists.
- **No `self` link.** Section "Links": a catalog served from a fixed URL
  SHOULD include an absolute `self` link on its root. STAC requires that a
  `self` link is absolute. The writer does not know the URL, so it writes no
  `self` link.
- **Relative hrefs with `./` or `../`.** Section "Links" permits relative
  links. `stac/examples/catalog.json` spells them `./name`.
- **Percent-encoded hrefs.** An href is a URI reference. RFC 3986 section 2.1
  encodes a space as `%20` and the UTF-8 bytes of `ó` as `%C3%B3`. Section
  "Links" requires that each link resolves. So the `item` link to
  `Estación Sur/Estación Sur.json` is `./Estaci%C3%B3n%20Sur/Estaci%C3%B3n%20Sur.json`.
- **`agents` and `describedby`.** Sections "AGENTS.md" and "README.md": every
  catalog and collection has an `AGENTS.md` behind `rel: "agents"` and a
  `README.md` behind `rel: "describedby"`, both with `type: text/markdown`.
  Section "Core Structure" puts both files in the directory of the catalog or
  collection, so the href is `./AGENTS.md` or `./README.md`.
- **Stale links.** The input root has a `child` link to a removed collection.
  The writer makes every local structural link, so that link is not in the
  output. `boundaries` has a `describedby` link to `./README.md` with the type
  `text/plain` and a title. The writer sets the type that section "README.md"
  requires and keeps the title, because a caller can put a `title` or an
  `hreflang` on that link (see best-practices/multilingual-catalogs.md).
- **Other links.** The `via` link on `boundaries` is not a link the writer
  makes, so it passes through unchanged after the writer's links.

The writer picks the link order because the spec sets none. The structural
links come first in the order root, parent, collection, then `child` and `item`
sorted by path. The `agents` and `describedby` links follow, then the other
links in their input order.

The JSON is two-space indented UTF-8 with literal non-ASCII characters and a
trailing newline, the same format as `portolan_core.json_io`.
