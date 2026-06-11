# ADR-0053: Mandatory human-readable titles and descriptions

## Status

Accepted

## Context

STAC Browser renders the `title` of `child` and `item` links directly. When a
link has no `title`, the browser must fetch every child's `collection.json`
just to display its name — the pergamino catalog showed `data.source.coop`
placeholders for several seconds while 100+ HTTP requests resolved.

Portolan-cli previously left titles unset on auto-created collections
(`description="Collection: <slug>"`, no title) and built `child`/`item` links
with only `rel`/`href`/`type`. STAC *allows* `title` on links but PySTAC only
emits it when the target object has a title, and several portolan code paths
build links as raw JSON, bypassing PySTAC entirely.

Feedback from Matthias Mohr (STAC Browser maintainer) and a portolan maintainer:

> "provide titles for the child (and item) links. This should definitely be
> part of the portolan spec."
> "we should also require human readable ones … not allow
> `publico_areas_programaticas_desarrollo_social` type — at least convert it to
> `Publico Areas Programaticas Desarrollo Social`."

Related: GitHub Issue [#502](https://github.com/portolan-sdi/portolan-cli/issues/502),
STAC Browser issue radiantearth/stac-browser#899.

## Decision

`title` and `description` are **mandatory** on every catalog, collection, and
item portolan generates, and every `child`/`item` link carries a `title`.
Titles must be **human-readable**.

1. **Auto-derive humanized titles.** When no human title is available, derive
   one from the slug: underscores/hyphens → spaces, title-case each word,
   preserving acronyms/CamelCase (`publico_arbolado` → `Publico Arbolado`,
   `IGN_layers` → `IGN Layers`). Only the leaf segment of a nested id is used
   ([ADR-0032](0032-nested-catalogs-with-flat-collections.md)). See
   `portolan_cli/humanize.py` (`humanize_slug`, `derive_title`).
2. **Default description = title** when no real description exists (never a
   `Collection: <slug>` placeholder).
3. **Human override.** `metadata.yaml` may set optional `title`/`description`
   keys, which take highest precedence over the auto-derived values
   ([ADR-0038](0038-metadata-yaml-enrichment.md)). Existing human-authored
   titles are never overwritten (SMART merge).
4. **Link propagation.** After catalog mutations, `ensure_link_titles()` walks
   the catalog and backfills `title` (+ `type`) onto every `child`/`item` link
   from its target.
5. **Enforce + repair.** `MandatoryTitlesRule` (ERROR severity) fails `portolan
   check` when a catalog/collection lacks a title/description, when a title is a
   raw slug (contains `_` or a `ns:` namespace prefix), or when a `child`/`item`
   link lacks a title. `portolan check --fix` repairs all of these.

The rule's "raw slug" predicate is intentionally narrower than the general
`is_technical_name` detector: it flags only the markers `humanize_slug`
removes (underscores, namespace prefixes). This guarantees `check --fix`
converges — a humanized title never re-triggers the rule, even for short ids
(`t502` → `T502`) that cannot be humanized further.

## Consequences

### Benefits

- STAC Browser renders child/item names instantly — no fan-out fetch.
- Catalogs are self-describing; no `Collection: <slug>` placeholders.
- Enforced by an automated rule, repairable in one command.

### Trade-offs

- Auto-derived titles are only as good as the slug; humans should override poor
  ones via `metadata.yaml`.
- Existing catalogs need a one-time `portolan check --fix` to become compliant.
- The link-title backfill is O(catalog) per add batch (run once per batch, not
  per collection, to stay linear).

## Alternatives Considered

### Leave titles optional, rely on PySTAC

**Rejected**: PySTAC only emits link titles when targets have titles, and
several portolan paths build links as raw JSON. Optional titles reproduce the
exact browser-performance problem this ADR fixes.

### Put title/description only in metadata.yaml

**Rejected**: `metadata.yaml` is human enrichment; requiring it for every
collection would block compliance on manual editing. Auto-derivation gives a
compliant baseline with metadata.yaml as the override.

## References

- [GitHub Issue #502: Require title attribute on child and item links](https://github.com/portolan-sdi/portolan-cli/issues/502)
- [ADR-0018: Metadata Generation Tiers](0018-metadata-generation-tiers.md)
- [ADR-0032: Nested Catalogs with Flat Collections](0032-nested-catalogs-with-flat-collections.md)
- [ADR-0038: Metadata YAML as Human Enrichment Layer](0038-metadata-yaml-enrichment.md)
- [ADR-0045: Styles as STAC Assets](0045-styles-as-stac-assets.md)
