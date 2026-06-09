# ADR-0054: ArcGIS folder recursion and nested-folder catalog structure

## Status
Adopted

## Context
`portolan extract arcgis` against a services root only discovered top-level
services, silently skipping every service nested in an ArcGIS Server folder, and
rejected folder URLs. ArcGIS Enterprise and federated servers organize services
into folders (some put ALL services in folders), so services-root extraction was
unusable against them. See issue #493.

## Decision
1. Services-root extraction recurses into folders by default. ArcGIS returns
   folder-qualified service names (e.g. `NationalDatasets/Property`), so URL
   construction and glob filters work unchanged. `--no-recurse` opts out.
2. Folders that error or require a token are logged as warnings and skipped; the
   run never aborts. Coverage (folders traversed/skipped, services found) is
   recorded on the extraction report and printed.
3. Folder URLs (`.../rest/services/<folder>`) parse as a new `SERVICES_FOLDER`
   type, scoped to that folder, with the base URL normalized to the true
   services root.
4. Token-secured folders/services are reachable when the user supplies a token
   (`--token`/`ARCGIS_TOKEN`) or username/password (minted via `generateToken`).
   This is a contained pass-through; the full auth design remains issue #311.
5. Folders map to nested subcatalogs; each folder segment becomes a slugified
   subcatalog directory and the service becomes a collection (single layer) or
   subcatalog (multi layer), consistent with ADR-0032 (nested catalogs for
   hierarchy). `--services`/`--exclude-services` match folder-qualified names.

## Consequences
- Enterprise/federated catalogs extract completely by default.
- Catalog trees gain a folder tier; deeper nesting for multi-layer services.
- Slug collisions are possible for names differing only by stripped characters
  (e.g. `turkiye` vs `turkiye-alt`), accepted for now, not mitigated.
- ImageServer-in-folder extraction and the full auth module are out of scope
  (issue #311).

## Related
Issues #493, #6, #492, #358, #311. Supersedes nothing. Builds on ADR-0032,
ADR-0048, ADR-0007.
