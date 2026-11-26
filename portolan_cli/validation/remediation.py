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
from typing import Any

from rashid_core.model import Severity


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
    "PTL-LNK-006": _auto(
        "links",
        "Repoint the structural link at the object it claims to reference.",
    ),
    # The three logo rules arrived with rashid 0.1.5. `portolan logo` and
    # `init --logo` write a conformant link, so a defect here means the link was
    # hand-edited or predates the command. Repairing it needs the image, which
    # `--fix` has no way to choose, so all three instruct.
    "PTL-LNK-007": _instruct(
        "Give the rel:'icon' link a browser-displayable image type: apng, avif, "
        "gif, jpeg, png, svg+xml, or webp. Re-run `portolan logo <file>` to have "
        "the type set from the file."
    ),
    "PTL-LNK-008": _instruct(
        "Title the rel:'icon' link so it has an accessible label; "
        "`portolan logo <file> --title` writes one."
    ),
    "PTL-LNK-009": _instruct(
        "Make the rel:'icon' href relative, so the catalog stays portable. "
        "`portolan logo <file>` publishes the image under _assets/ and links it "
        "relatively."
    ),
    # A translated tree sits outside the containment tree. Choosing between
    # dropping the link and moving the object is the publisher's call, so the
    # `links` fixer does not guess.
    "PTL-LNK-010": _instruct(
        "Link the alternate-language object as an alternate, not as a child or an item; "
        "a translated tree stays outside the containment tree."
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
    # Not AUTO despite looking mechanical. The `assets` fixer backfills a media
    # type only when one is missing, and never overwrites a type already there.
    # A wrong type here is a claim about the bytes, and correcting it without
    # reading them would be a guess.
    "PTL-AST-006": _instruct(
        "Set the raster data asset's type to "
        "'image/tiff; application=geotiff; profile=cloud-optimized', or convert "
        "the file to a COG if it is not one, with `portolan check --fix --geo-assets`."
    ),
    "PTL-AST-007": _instruct(
        "Link the collection's primary data with an asset that carries the 'data' role."
    ),
    "PTL-AST-008": _instruct("Give the item at least one asset with the 'data' role."),
    "PTL-AST-009": _instruct(
        "Give the PMTiles asset the 'visual' role and link it to the collection's GeoParquet."
    ),
    "PTL-AST-010": _instruct(
        "Give the asset a title and a description that tell a reader what the file holds."
    ),
    # A partitioned dataset needs an absolute partition:glob that clients can
    # list, which a local catalog does not know before it is published.
    "PTL-AST-012": _instruct(
        "Link each data file in its own asset or item, or move the glob into an absolute "
        "partition:glob URL. A data asset href names one file."
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
    # The rule that would have caught the raster v1.1.0 drift in #654. It is a
    # warning upstream and it instructs here, for the same reason: a catalog
    # published before the registry moved is behind, not broken. Rewriting the
    # URI also changes which schema the object must satisfy, which is a decision
    # a sweep should not make on the operator's behalf.
    "PTL-CNF-004": _instruct(
        "Move the declared extension URI to the version the Portolan registry "
        "pins, then re-run generation so the fields match that version's schema."
    ),
    "PTL-CNF-005": _instruct(
        "Declare the newest Portolan profile version rashid bundles, after the catalog meets "
        "its requirements."
    ),
    # ---- visualization ----
    "PTL-VIZ-001": _auto(
        "thumbnail",
        "Register a thumbnail (png, jpeg or webp) asset for the collection.",
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
        "; run `portolan add --pmtiles` with tippecanoe on PATH."
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
    "PTL-VIZ-007": _instruct(
        "Build an 8-bit RGB overview COG of the whole collection and register it as a "
        "collection-level asset with the 'visual' and 'overview' roles."
    ),
    "PTL-VIZ-008": _instruct(
        "Give the overview asset a datetime, or a start_datetime and end_datetime interval."
    ),
    # ---- partitions ----
    # fixer `partition` wraps partitioning.py's schema read
    "PTL-PRT-001": _auto(
        "partition",
        "Populate the partition: fields from the Hive layout on disk.",
    ),
    "PTL-PRT-002": _instruct(
        "Set partition:glob to the absolute bucket URL of the partition files, such as "
        "s3://bucket/data/*.parquet, so clients can list them."
    ),
    # ---- classification: class lists describe the data, so only the publisher knows them ----
    "PTL-CLS-001": _instruct(
        "Declare the classification extension on the object that carries classification:classes."
    ),
    "PTL-CLS-002": _instruct(
        "Give each classification:classes entry a title, in the extension's class object form."
    ),
    "PTL-CLS-003": _instruct(
        "Mark a class as nodata exactly when its value is the band's nodata value."
    ),
    "PTL-CLS-005": _instruct(
        "Move the class list onto a single-band data asset, or onto its band in the bands array."
    ),
    "PTL-CLS-006": _instruct(
        "Put the items' class lists in the same place in the collection's item_assets."
    ),
    # ---- versions: the history is the publisher's record ----
    "PTL-VER-001": _instruct(
        "Repair the version links so each resolves, neighbours agree, and latest names a head."
    ),
    # ---- catalogs: how to group children is the publisher's decision ----
    "PTL-CAT-001": _instruct(
        "Group the children under subcatalogs; twenty or more ungrouped children make "
        "the catalog hard to read."
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
    "PTL-COL-005": _instruct(
        "Publish the items the collection implies; the item JSON is the normative "
        "representation and a mirror is a derived copy of it."
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
    "PTL-DAT-017": _instruct(
        "Document the vector asset's columns with table:columns, on the collection or in "
        "the item's properties; re-run `portolan add` to have them read from the file."
    ),
    "PTL-DAT-018": _instruct(
        "Rewrite the collection overview COG as 8-bit RGB, with an optional alpha band."
    ),
    "PTL-DAT-019": _instruct(
        "Declare the scene's nodata value, or add an alpha band or a mask, so clients do not "
        "draw empty pixels as black."
    ),
    "PTL-DAT-020": _instruct(
        "Describe each table:columns entry. For a mirror whose source has no descriptions, "
        "say so in the collection README."
    ),
    "PTL-DAT-021": _instruct(
        "Align the class list with the COG, with color_hint values that match its color table "
        "and classes that fit its bands."
    ),
    "PTL-DAT-023": _instruct(
        "Give the native geometry column its CRS as inline PROJJSON or a projjson:<key> reference."
    ),
    # ---- generic, structural, schema: the object is malformed, not misconfigured ----
    "PTL-GEN-000": _instruct(
        "Create catalog.json at the catalog root; without it there is nothing to validate."
    ),
    "PTL-GEN-001": _instruct("Repair the file so it parses as JSON."),
    "PTL-GEN-002": _instruct(
        "Raise the document limit or narrow the check, so rashid reads the whole tree."
    ),
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
    "PTL-SCH-002": _instruct(
        "Remove or correct the profile schema mapping that matched no validated document."
    ),
    # ---- extensions: the object breaks a STAC extension schema it declares ----
    "PTL-EXT-000": _instruct(
        "Re-run with network access so rashid can fetch the extension schema, or declare an "
        "extension version rashid ships."
    ),
    "PTL-EXT-001": _instruct(
        "Correct the object against the schema of the extension it declares; the schema error "
        "names the failing field."
    ),
    "PTL-EXT-002": _instruct(
        "Declare an extension the Portolan registry lists, or accept that rashid did not "
        "validate the object against this extension's schema."
    ),
    # ---- live: server configuration, except a file the push did not publish ----
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
    # No header list here. The required list changes between rashid and spec
    # versions, and rashid's finding message names the headers the host omits.
    "PTL-LIV-004": _external(
        "Configure the host to expose the range response headers that the finding names "
        "via Access-Control-Expose-Headers."
    ),
    "PTL-LIV-005": _external(
        "Configure the host to answer CORS preflight (OPTIONS) requests for asset URLs."
    ),
    # The host lacks a file the catalog links. The fix is a push, not a host
    # setting, so this live rule is INSTRUCT (issue #935).
    "PTL-LIV-006": _instruct(
        "Run `portolan push` again so the host serves every file a link names; if the file is "
        "missing locally too, restore it or remove the link."
    ),
    "PTL-LIV-007": _instruct(
        "Point the root self link at the base URL the catalog is published under."
    ),
    "PTL-LIV-008": _external(
        "Configure the host to serve assets and link targets over https with no redirect to http."
    ),
    # ---- zarr: store layout is set when the store is written ----
    "PTL-ZAR-001": _instruct("Point the asset at a readable Zarr v3 store."),
    "PTL-ZAR-002": _instruct(
        "Declare the spatial, proj, and multiscales conventions in the store and conform to them."
    ),
    "PTL-ZAR-003": _external(
        "Configure the host to allow a cross-origin read of the Zarr store metadata."
    ),
    "PTL-ZAR-004": _instruct("Type the asset as Zarr v3 and point its href at a group."),
    "PTL-ZAR-005": _instruct("Give every renderable array an affine spatial:transform."),
    "PTL-ZAR-006": _instruct("Rewrite the arrays with the spatial axes last, Y before X."),
    "PTL-ZAR-007": _instruct("Rewrite the arrays row-major, with no transpose codec."),
    "PTL-ZAR-008": _instruct("Record proj:code, or proj:projjson, in the store attributes."),
    "PTL-ZAR-009": _instruct("Add a pyramid whose coarsest level fits in 4 chunks on each axis."),
    "PTL-ZAR-010": _instruct(
        "Give every multiscales layout entry a spatial:transform and a spatial:shape."
    ),
    "PTL-ZAR-011": _instruct("Rebuild the pyramid so each level steps by a factor of 2."),
    "PTL-ZAR-012": _instruct(
        "Declare cube:variables for a store with more than one data variable."
    ),
    "PTL-ZAR-013": _instruct("Give every renderable array band statistics or a renders rescale."),
    "PTL-ZAR-014": _instruct("Declare nodata on the bands entry of every renderable array."),
    "PTL-ZAR-015": _instruct(
        "Rechunk or add pyramid levels so a client reads no more than 8 samples per drawn sample."
    ),
    "PTL-ZAR-016": _instruct("Rechunk the non-spatial dimensions to a chunk length of 1."),
    "PTL-ZAR-017": _instruct("Rechunk to 256 to 1024 samples on X and Y."),
    "PTL-ZAR-018": _instruct("Shard the non-spatial dimensions by a product of 8 or less."),
    "PTL-ZAR-019": _instruct("Use only codecs that have a JavaScript implementation."),
}


def remediation_for(rule_id: str) -> Remediation:
    """Look up how ``rule_id`` is remediated, never raising on an unknown id."""
    return RULE_REMEDIATION.get(rule_id, DEFAULT_REMEDIATION)


def remediation_of(finding: Any) -> Remediation:
    """How one finding is remediated: its rule's row, except an INFO is never AUTO.

    An INFO is a note, not a defect, so no fixer runs for it and it never
    counts toward ``auto`` or as a survivor. rashid 0.4 reports PTL-DAT-006 as
    an INFO when a file has too few rows to judge its order. Read as AUTO, that
    note selected ``convert`` and kept ``auto`` above zero after every ``--fix``.
    It keeps its requirement sentence as an INSTRUCT row.
    """
    remediation = remediation_for(finding.rule_id)
    if remediation.bucket is Bucket.AUTO and finding.severity is Severity.INFO:
        return Remediation(Bucket.INSTRUCT, None, remediation.requirement)
    return remediation
