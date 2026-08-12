"""How `portolan check` remediates each rashid rule id.

rashid says what is wrong. This table says who fixes it:

- ``AUTO`` — mechanical. ``--fix`` applies a fixer and the defect disappears.
- ``INSTRUCT`` — judgment. Portolan cannot invent a license, a provider, or a
  readable title, so it emits the requirement and the agent or human acts.
- ``EXTERNAL`` — not in the catalog at all. Range and CORS defects are settings
  on the server hosting the published bytes.

The ``fixer`` key on an AUTO row is what ``--fix`` dispatches on:
:data:`portolan_cli.validation.fixers.FIXERS` maps each key to the repairer that
runs, and ``fixers.auto_fixer_keys`` selects the keys by membership in that
dict. The key is therefore a live identifier, not a label — renaming one here
without renaming it in ``FIXERS`` silently drops the rule out of the AUTO
selection and ``--fix`` stops repairing it, with no error anywhere. That is why
``TestRegistryCompleteness`` in ``tests/unit/validation/test_fixers.py`` pins the
correspondence in both directions.

``--fix`` still also drives ``scan.check.run_fix_workflow`` alongside the
registry, but for item freshness against the filesystem, which no rule reports.

``requirement`` is the imperative sentence shown for a finding, phrased so it
reads as an instruction on its own, without the rule id or the message.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Bucket(Enum):
    """Who resolves a finding."""

    AUTO = "auto"
    INSTRUCT = "instruct"
    EXTERNAL = "external"


@dataclass(frozen=True)
class Remediation:
    """The remediation policy for one rule id.

    Attributes:
        bucket: Who resolves it.
        fixer: Fixer-registry key, set only on AUTO rows.
        requirement: Imperative sentence describing what conformance demands.
    """

    bucket: Bucket
    fixer: str | None
    requirement: str


#: Fallback for a rule id this table has not mapped — a rule added upstream
#: before the table caught up. Unmapped never means unhandled: the finding still
#: reaches the user, it just carries no fixer and no extra requirement text.
DEFAULT_REMEDIATION = Remediation(Bucket.INSTRUCT, None, "")


def _auto(fixer: str, requirement: str) -> Remediation:
    return Remediation(Bucket.AUTO, fixer, requirement)


def _instruct(requirement: str) -> Remediation:
    return Remediation(Bucket.INSTRUCT, None, requirement)


def _external(requirement: str) -> Remediation:
    return Remediation(Bucket.EXTERNAL, None, requirement)


RULE_REMEDIATION: dict[str, Remediation] = {
    # ---- files: scaffolding and links are mechanical, prose is not ----
    # fixer `required_files` composes readme.ensure_readmes + metadata.fix.repair_agents_md.
    # It subsumes `agents` and `readme`: when this row fires, fixers.auto_fixer_keys
    # drops those two from the selection so the same repair does not run three times.
    "PTL-FIL-001": _auto(
        "required_files",
        "Every catalog and collection directory must contain README.md and AGENTS.md.",
    ),
    # fixer `agents` wraps metadata.fix.repair_agents_md
    "PTL-FIL-002": _auto(
        "agents",
        "Link AGENTS.md with rel='agents' and type='text/markdown'.",
    ),
    # fixer `readme` wraps readme.ensure_readmes
    "PTL-FIL-003": _auto(
        "readme",
        "Link README.md with rel='describedby' and type='text/markdown'.",
    ),
    "PTL-FIL-004": _instruct(
        "Write README.md content: it must open with a title heading and describe the data."
    ),
    "PTL-FIL-005": _instruct(
        "Extend the collection README with its license terms and where the data came from."
    ),
    # ---- titles: a missing title can be generated, a bad one cannot ----
    # fixer `titles` wraps metadata.fix.repair_titles_and_links
    "PTL-TTL-001": _auto(
        "titles",
        "Give every catalog.json and collection.json a non-empty title and description.",
    ),
    "PTL-TTL-002": _instruct(
        "Replace the machine-generated title with prose a reader recognizes; "
        "a de-slugged identifier is not a title."
    ),
    "PTL-TTL-003": _auto("titles", "Give every child and item link a title."),
    # ---- links: wholly derivable from the file tree ----
    # fixer `links` is new in Phase 3; it rewrites the structural link block
    "PTL-LNK-001": _auto(
        "links",
        "Add the required structural links: root and parent on catalogs and collections, "
        "root, parent, and collection on items.",
    ),
    "PTL-LNK-002": _auto(
        "links",
        "Add a child or item link for every object contained in this one.",
    ),
    "PTL-LNK-003": _auto(
        "links",
        "Set structural link types: application/json, or application/geo+json for item links.",
    ),
    "PTL-LNK-004": _auto(
        "links",
        "Make structural link hrefs relative so the catalog stays portable.",
    ),
    "PTL-LNK-005": _auto("links", "Remove the self link; a SELF_CONTAINED catalog omits it."),
    "PTL-LNK-006": _auto(
        "links",
        "Repoint the structural link at the object it claims to reference.",
    ),
    # ---- bbox: derivable from the assets it summarizes ----
    # fixer `bbox` wraps bbox.py's extent computation
    "PTL-BBX-001": _auto(
        "bbox",
        "Recompute the bbox from the collection's assets as finite WGS84 coordinates "
        "with south <= north.",
    ),
    # ---- assets: field backfill and checksums are mechanical, hosting is not ----
    # fixer `assets` is new in Phase 3; it backfills type and roles from the extension registry
    "PTL-AST-001": _auto(
        "assets", "Give every asset an href, a media type, and at least one role."
    ),
    "PTL-AST-002": _instruct(
        "Publish the asset over https and point the href at the public URL; "
        "a browser cannot fetch an s3:// href."
    ),
    # fixer `checksum` wraps sync.checksums
    "PTL-AST-003": _auto("checksum", "Record file:size and file:checksum on every asset."),
    "PTL-AST-004": _auto("checksum", "Encode file:checksum as a multihash, not a bare digest."),
    "PTL-AST-005": _instruct(
        "Move the asset onto a collection or an item; a catalog organizes, it does not carry data."
    ),
    # ---- conformance ----
    # fixer `schema_uri` wraps catalog.ensure_schema_uris
    "PTL-CNF-001": _auto(
        "schema_uri",
        "Declare the versioned Portolan schema URI in stac_extensions.",
    ),
    "PTL-CNF-002": _auto(
        "schema_uri",
        "Align this object's Portolan schema URI with the root catalog's.",
    ),
    "PTL-CNF-003": _instruct(
        "Declare the STAC version extension on the versioned collection, "
        "then populate its version fields."
    ),
    # ---- visualization ----
    "PTL-VIZ-001": _instruct(
        "Render a thumbnail (png or jpeg) for the collection and register it as an asset; "
        "`portolan add` does this when the [thumbnails] extra is installed."
    ),
    "PTL-VIZ-002": _instruct(
        "Author style assets for the visual derivative; a style encodes cartographic intent "
        "Portolan cannot guess."
    ),
    # fixer `pmtiles` wraps metadata.fix.repair_pmtiles_links
    "PTL-VIZ-003": _auto(
        "pmtiles",
        "Register the PMTiles asset with a rel='pmtiles' link (web-map-links v1.3.0).",
    ),
    "PTL-VIZ-004": _instruct(
        "Generate a PMTiles derivative for this large vector collection so browsers can render it "
        "; run `portolan viz` with the [pmtiles] extra."
    ),
    # fixer `styles` is new in Phase 3; it corrects the style asset media type
    "PTL-VIZ-005": _auto(
        "styles",
        "Type the style asset application/vnd.mapbox.style+json; a PMTiles style is a "
        "MapLibre GL style file.",
    ),
    # Which of several styles is the default is a judgment about presentation,
    # so Portolan states the requirement rather than picking one.
    "PTL-VIZ-006": _instruct(
        "Add 'default' to the roles of exactly one style asset when a collection "
        "registers more than one style."
    ),
    # ---- partitions ----
    # fixer `partition` wraps partitioning.py's schema read
    "PTL-PRT-001": _auto(
        "partition",
        "Populate the partition: fields from the Hive layout on disk.",
    ),
    # ---- collections: restructuring a layout is a decision, not an edit ----
    "PTL-COL-001": _instruct(
        "Expose the single file as a collection-level asset rather than an item."
    ),
    "PTL-COL-002": _instruct(
        "Flatten the nesting: a collection may not contain another collection. "
        "Promote the inner collection to a subcatalog."
    ),
    "PTL-COL-003": _instruct(
        "Rename the collection to a slug that matches its directory and is unique in the catalog."
    ),
    "PTL-COL-004": _instruct(
        "Move each raster scene onto its own item; only a single-COG collection carries a scene "
        "itself."
    ),
    # ---- temporal: an extent is a fact about the data, not about the file ----
    "PTL-TMP-001": _instruct(
        "Record the item's datetime, or its start_datetime/end_datetime interval, "
        "from the source data."
    ),
    "PTL-TMP-002": _instruct(
        "Correct the datetime fields: RFC 3339, with start_datetime no later than end_datetime."
    ),
    # ---- providers: authorship is a human fact ----
    "PTL-PRV-001": _instruct(
        "Name the organization that produced the data as a provider with the producer role."
    ),
    "PTL-PRV-002": _instruct("Declare exactly one provider with the host role and list it last."),
    "PTL-PRV-003": _instruct("Give the host provider a url or an email."),
    # ---- license: a legal fact ----
    "PTL-LIC-001": _instruct(
        "Declare the license as an SPDX identifier, or 'other' with a rel='license' link."
    ),
    "PTL-LIC-002": _instruct("Add a rel='license' link to the license text for license 'other'."),
    "PTL-LIC-003": _instruct(
        "Replace the deprecated 'proprietary' license with an SPDX identifier or 'other'."
    ),
    # ---- provenance: only the publisher knows the upstream ----
    "PTL-PRO-001": _instruct(
        "Add a rel='via' link (type text/html) to the original source this collection mirrors."
    ),
    "PTL-PRO-002": _instruct(
        "Add a rel='canonical' link to the upstream STAC object, when the source publishes one."
    ),
    "PTL-PRO-003": _instruct(
        "Record the sync time in a top-level RFC 3339 'updated' field on the mirror."
    ),
    "PTL-PRO-004": _instruct(
        "Remove the via/canonical upstream links: an official collection is the source, "
        "not a mirror."
    ),
    # ---- item mirror: regenerated wholesale from the items ----
    # fixer `item_mirror` wraps stac_parquet's items.parquet generation
    "PTL-MIR-001": _auto(
        "item_mirror",
        "Generate the items.parquet mirror for this collection.",
    ),
    "PTL-MIR-002": _auto(
        "item_mirror",
        "Register items.parquet as an asset so consumers can find the mirror.",
    ),
    # ---- data pass: recompute or re-convert, except where a choice is implied ----
    "PTL-DAT-000": _instruct(
        "Install the geospatial stack (pyarrow, rasterio, rio-cogeo, pyproj) so the data pass "
        "can read asset bytes; it degraded to this warning instead."
    ),
    "PTL-DAT-001": _auto("checksum", "Recompute file:checksum from the asset's current bytes."),
    "PTL-DAT-002": _auto("checksum", "Recompute file:size from the asset's current bytes."),
    "PTL-DAT-003": _auto("convert", "Convert the asset to the cloud-native format it declares."),
    "PTL-DAT-004": _auto("convert", "Rewrite the raster as a valid COG."),
    "PTL-DAT-005": _instruct(
        "Reconcile the asset's bytes with the metadata describing them; they disagree about the "
        "data itself, which no rewrite can settle."
    ),
    "PTL-DAT-006": _auto("convert", "Rewrite the GeoParquet with spatially ordered row groups."),
    "PTL-DAT-007": _auto(
        "convert", "Rewrite the GeoParquet so every row group carries statistics."
    ),
    "PTL-DAT-008": _auto("convert", "Rewrite the GeoParquet with row groups in the target size."),
    "PTL-DAT-009": _instruct(
        "Recompute the raster statistics from the source data; the COG's stored values are absent "
        "or wrong."
    ),
    "PTL-DAT-010": _instruct(
        "Investigate the raster's valid-data percentage: a nodata-dominated band usually means the "
        "wrong extent or the wrong nodata value, not a conversion setting."
    ),
    "PTL-DAT-011": _auto("convert", "Rebuild the COG's overview pyramid."),
    "PTL-DAT-012": _instruct(
        "Rewrite the file against a supported GeoParquet version; the declared version predates "
        "what consumers read."
    ),
    "PTL-DAT-013": _auto("convert", "Rewrite the COG with the 512x512 tile size."),
    "PTL-DAT-014": _instruct(
        "Align the partition schema with the files on disk; the layout and the declaration "
        "disagree about the partitioning keys."
    ),
    "PTL-DAT-015": _instruct(
        "Declare the non-geo table as tabular data; it carries no geometry column."
    ),
    "PTL-DAT-016": _instruct(
        "Regenerate the items.parquet mirror from the current items; its contents have drifted "
        "from the item files."
    ),
    # ---- generic, structural, schema: the object is malformed, not misconfigured ----
    "PTL-GEN-000": _instruct(
        "Create catalog.json at the catalog root; without it there is nothing to validate."
    ),
    "PTL-GEN-001": _instruct("Repair the file so it parses as JSON."),
    "PTL-STR-000": _instruct(
        "Reinstall rashid: the STAC schemas it ships could not be loaded, so the structural pass "
        "degraded to this warning."
    ),
    "PTL-STR-001": _instruct(
        "Correct the object against STAC 1.1.0; the structural error names the failing field."
    ),
    "PTL-SCH-000": _instruct(
        "Point the object at a Portolan schema version rashid ships, or re-run with "
        "network access so it can fetch that version."
    ),
    "PTL-SCH-001": _instruct(
        "Correct the object against the Portolan profile schema; the schema error names the "
        "failing field."
    ),
    # ---- live: server configuration, never a catalog edit ----
    "PTL-LIV-000": _instruct(
        "Re-run the live pass with a reachable host; it could not probe the published assets."
    ),
    "PTL-LIV-001": _external(
        "Configure the host to honor HTTP Range requests; COGs and GeoParquet are unreadable "
        "without them."
    ),
    "PTL-LIV-002": _external(
        "Configure the host to return Content-Length on HEAD requests for asset URLs."
    ),
    "PTL-LIV-003": _external(
        "Configure the host to send Access-Control-Allow-Origin so browsers can read the assets."
    ),
    "PTL-LIV-004": _external(
        "Configure the host to expose Content-Length and Content-Range via "
        "Access-Control-Expose-Headers."
    ),
    "PTL-LIV-005": _external(
        "Configure the host to answer CORS preflight (OPTIONS) requests for asset URLs."
    ),
}


def remediation_for(rule_id: str) -> Remediation:
    """Look up how ``rule_id`` is remediated, never raising on an unknown id."""
    return RULE_REMEDIATION.get(rule_id, DEFAULT_REMEDIATION)
