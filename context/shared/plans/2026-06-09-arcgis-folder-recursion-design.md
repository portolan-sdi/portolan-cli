# Design, folder-aware ArcGIS services-root extraction

**Issue:** [portolan-sdi/portolan-cli#493](https://github.com/portolan-sdi/portolan-cli/issues/493)
**Branch:** `feature/arcgis-folder-recursion`
**Date:** 2026-06-09
**Status:** Approved, ready for implementation plan

## Problem

`portolan extract arcgis <URL>` against a services root (`.../rest/services`) only
discovers and extracts services at the **top level**. It reads the `folders` array
but never traverses it, so every service nested in a folder is silently skipped, with
no error or warning. Folder URLs (`.../rest/services/<folder>`) are also rejected by
the URL parser. This makes the services-root mode unusable against ArcGIS Enterprise
and federated servers, where organizing services into folders is the norm.

Verified against `main` (`1.0.0a0`):

- `portolan_cli/extract/arcgis/discovery.py`, `discover_services()` (lines ~235-279)
  reads `data.get("folders", [])` but never fetches `<root>/<folder>?f=json`.
- `portolan_cli/extract/arcgis/orchestrator.py`, both `list_services()` (~131) and
  `_discover_and_filter_services()` (~783) call `discover_services(..., return_folders=True)`
  then **discard** the folders.
- `portolan_cli/extract/arcgis/url_parser.py`, `_SERVICES_ROOT_PATTERN = r"/rest/services/?$"`
  matches only the bare root, so a folder URL raises `InvalidArcGISURLError [PRTLN-EXT001]`.

## Investigation findings (live servers)

Two live, anonymously readable servers were probed with `curl`.

1. South Africa NSPDR, `https://nspdr.dlrrd.gov.za/server/rest/services`
   - 13 top-level services (mostly GPServer export tools, 2 MapServers).
   - 7 folders. `NationalDatasets` alone holds 8 MapServers, 100+ feature layers.
   - Querying a folder returns names **already root-qualified**, e.g.
     `NationalDatasets/Property`. Nested `folders` is empty.
2. JRC federated server, `https://arcgis-maps.jrc.ec.europa.eu/federated_server/rest/services`
   - **Zero top-level services.** All data lives in 14 folders.
   - Folder JSON returns root-qualified names, e.g. `ecml/active_faults`, and
     `folders: []` (no nesting).
   - Non-ASCII names exist, e.g. `ecml/eq_türkiye_20230206_m78_mmi`.

Consequences for the design:

- Recursion must be **on by default** for services-root extraction, or the JRC server
  (and any folder-organized Enterprise server) yields nothing.
- The API returns root-qualified names, so `ServiceInfo.get_url(root_base)` already
  builds correct URLs, and `fnmatch`-based filters already match `Folder/Service`.
- Folders are single level per the ArcGIS REST spec, but traversal is written
  defensively (queue based, depth guarded) per the issue request.
- `_slugify` already collapses non-ASCII and `/` into `_`, so names are filesystem safe,
  with a small collision risk (`türkiye` vs `turkiye`) noted, not addressed in v1.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Recursion trigger | Default-on for services-root extraction | Folder-organized servers are common, silent partial results are the bug |
| Token-secured folders | Use auth if provided, else skip with warning | User can reach secured data, runs never abort |
| Auth scope | `--token` plus `--username`/`--password` to token via `generateToken`, plus `ARCGIS_TOKEN` env | Matches operator workflows, contained pass-through, full design stays in #311 |
| Catalog structure | Nested by folder, folder becomes a subcatalog tier | ADR-0032 favors nested catalogs for hierarchy |
| Structure governance | New ADR in this repo, no portolan-spec issue | ADR-0048, CLI repo is spec source of truth |
| Scope | All 5 issue points plus auth refinement | Cohesive single spec |

## Architecture

The change is contained to the arcgis extract subsystem
(`portolan_cli/extract/arcgis/`) plus CLI wiring in `cli.py`, one new ADR, and tests.
The library-wraps-CLI boundary (ADR-0007) is preserved, all logic stays in the
extract modules and the CLI only parses flags and delegates.

### Unit 1, discovery recursion (`discovery.py`)

- Add `recurse: bool = False` and `auth: ArcGISCredentials | None = None` to
  `discover_services()`. Default stays non-recursive so existing direct callers and
  unit tests keep their contract. The **orchestrator** opts into `recurse=True`.
- New `FolderTraversal` dataclass capturing `visited: list[str]`,
  `skipped: list[tuple[str, str]]` (folder, reason), `service_count: int`.
- New internal `_discover_services_recursive(root_url, service_types, auth, timeout,
  max_depth=2)` doing a queue-based BFS, seeded with the root, enqueuing each
  `folders` entry, fetching `<root>/<folder>?f=json`, merging `services`. Depth guard
  prevents pathological loops on misbehaving servers.
- `discover_services(..., recurse=True, return_folders=True)` returns
  `(services, FolderTraversal)`. Keep the existing non-recursive return shapes via
  overloads so type checking stays precise.
- `_fetch_json()` is extended to raise a typed discovery error when the body is a
  JSON `{"error": {"code": ..., "message": ...}}` object, since ArcGIS returns HTTP
  200 with an embedded error for token-required cases.

### Unit 2, auth (`extract/arcgis/auth.py`, new)

- `@dataclass ArcGISCredentials` with `token: str | None`, `username: str | None`,
  `password: str | None`.
- `resolve_token(creds, base_url, timeout) -> str | None`. If `token` is set, return
  it. If username/password are set, discover the token endpoint from
  `<server>/rest/info?f=json` (`authInfo.tokenServicesUrl`, fallback to
  `<root>/../tokens/generateToken`) and POST credentials to mint a token. Errors raise
  a typed `ArcGISAuthError` (added to `errors.py`).
- `apply_auth(url, token) -> str` appends `token=<token>` to a request URL.
- Token threads into every discovery request and into `gpio.extract_arcgis` via its
  auth/token parameter, feature-detected with `inspect.signature` exactly like the
  existing `max_workers` detection in `_extract_single_layer`.
- Credential sources resolved in CLI, precedence `--token` > `--username`/`--password`
  > `ARCGIS_TOKEN` env. Cross-reference #311 in code comments, this is the minimal
  contained version, not the full module.

### Unit 3, graceful skip

- During recursion, a folder fetch that raises (non-2xx or embedded error) is caught,
  logged via `output.warn`, appended to `FolderTraversal.skipped`, and traversal
  continues. Never aborts.
- The existing per-service layer-discovery skip in `_collect_layers_from_services`
  (already a try/except continue) is retained and its failures fold into the coverage
  report.

### Unit 4, URL parser accepts folder URLs (`url_parser.py`)

- Add `ArcGISURLType.SERVICES_FOLDER`.
- After the FeatureServer/MapServer/ImageServer patterns fail and before the bare-root
  pattern, match `/rest/services/(.+?)/?$` where the captured remainder contains **no**
  known server-type segment. That remainder is the folder path.
- Return `ParsedArcGISURL` with `url_type=SERVICES_FOLDER`, `base_url` normalized to the
  true `.../rest/services` root (strip the folder), and the folder captured in a new
  `folder: str | None` field. This keeps `ServiceInfo.get_url(base_url)` correct because
  names stay root-qualified.
- `is_single_service` stays `False` for `SERVICES_FOLDER` so it routes through the
  services-root extraction path, scoped to the one folder.
- Document the ambiguity rule, a trailing path after `rest/services` with no server-type
  segment is treated as a folder.

### Unit 5, nested-by-folder structure (`orchestrator.py`)

- In `_extract_services_root`, replace `service_slug = _slugify(service.name)` with a
  split on `/`. Each leading segment (the folder path) becomes a slugified subcatalog
  directory, the final segment is the service. Resulting layout:
  - single-layer service, `output/<folder...>/<service>/<service>.parquet`
  - multi-layer service, `output/<folder...>/<service>/<layer>/<layer>.parquet`
- `_auto_init_catalog` already calls `init_catalog` then `add_files`, which infers the
  catalog tree from the on-disk directory layout and creates subcatalog `catalog.json`
  files (the existing multi-layer path proves this). The plan must **verify** that a
  folder tier with multiple services produces a valid intermediate `catalog.json`, and
  add coverage if not.
- For a `SERVICES_FOLDER` URL, the same code path runs scoped to the single folder, so
  the output is rooted at that folder's subcatalog naturally.

### Unit 6, filters and coverage reporting

- Filters already operate on `s.name` and use `fnmatch`, so `--services "ecml/*"`
  matches `ecml/active_faults`. Add tests and document, no behavior change needed.
- `ServicesRootDiscoveryResult` and the extraction report/summary gain coverage fields
  fed from `FolderTraversal`, folders traversed, folders skipped with reasons, services
  found.
- `--list-services` output and the post-extraction summary print coverage so a partial
  result is never mistaken for complete. `report.py` carries the fields, CLI renders
  them (text and `--json`).

### Unit 7, ADR

- New `context/shared/adr/0053-arcgis-folder-recursion-and-structure.md` recording, the
  folder to subcatalog mapping, recursion-by-default for services-root extraction, the
  graceful-skip-unless-auth behavior, and the minimal token pass-through with a pointer
  to #311.
- Register `0053` in the root `CLAUDE.md` ADR index (the validator reads it).

## CLI surface (new options on `extract arcgis`)

- `--token TEXT`, ArcGIS token, also `ARCGIS_TOKEN` env.
- `--username TEXT` and `--password TEXT`, mint a token via `generateToken`.
- `--no-recurse`, opt out of folder recursion for services-root URLs (default recurses).
- No change to `--services`/`--exclude-services`/`--filter` semantics, they already match
  folder-qualified names.

## Error handling

- `ArcGISAuthError` (new, `errors.py`), token minting or invalid credentials.
- Embedded ArcGIS error bodies raise the existing `ArcGISDiscoveryError` for the failing
  endpoint, caught and converted to a skip during folder traversal.
- Folder URL parsing failures keep raising `InvalidArcGISURLError [PRTLN-EXT001]`.
- All user-facing messages go through `output.py` (`warn`/`info`/`error`), diagnostics
  through `logging`, per the python rules.

## Testing strategy (TDD, network mocked)

Unit, mocked `httpx`:

- recursion merges folder services and preserves root-qualified names
- depth guard stops at `max_depth`, nested folders beyond it are not fetched
- embedded `{"error": {...}}` body triggers skip, traversal continues, recorded in
  `FolderTraversal.skipped`
- auth, `--token` appended to requests, username/password resolves via mocked
  `generateToken`, `ArcGISAuthError` on failure
- url_parser, folder URL parses to `SERVICES_FOLDER` with normalized root base and
  captured folder, ambiguous and Unicode folder names, plus regression that real
  service URLs still parse as before
- nested path construction for single and multi layer services, folder tier produces a
  subcatalog
- filter matches `ecml/*` against qualified names
- coverage record populated and rendered (text and JSON)

Integration, `@pytest.mark.network` (or `realdata`, mocked locally, real in nightly):

- against the SA NSPDR and JRC roots, assert folders are traversed, qualified names are
  returned, and the JRC root (zero top-level services) yields a non-empty service list.

Existing tests to update:

- any test asserting `discover_services` returns top-level only stays valid for the
  non-recursive default, add new tests for `recurse=True`.
- orchestrator tests that assumed top-level-only services-root extraction get folder
  fixtures.

## Out of scope

- GPServer and Utilities services, still filtered to FeatureServer/MapServer.
- ImageServer-in-folder extraction (raster path), the recursion is wired for the vector
  path, ImageServer folder support can follow.
- The full unified auth module, owned by #311. This spec ships only a contained token
  pass-through.

## Related issues

- #493, this bug.
- #6, parent feature, full ArcGIS Server to Portolan conversion, the folder-filtering
  comment depends on qualified names delivered here.
- #492, upstream sync, depends on folder traversal and qualified names.
- #358, flatten single-layer services, structure precedent.
- #311, unified auth, this spec is the minimal contained precursor.
