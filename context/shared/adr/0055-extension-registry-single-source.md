# ADR-0055: Single-source the recognized-extension vocabulary via a typed registry

## Status
Accepted

## Context

Portolan classifies files by extension in many places. That vocabulary had
drifted across five surfaces (issue #558, pre-sprint audit finding C6):

- `spec/extensions.md` — the human-facing doc
- `formats.py` — cloud-native / convertible / unsupported / routing frozensets,
  display names, error messages
- `constants.py` — geospatial / tabular sets, sidecar patterns
- `scan_classify.py` — the ten scan-category frozensets
- `add.py` — `_MEDIA_TYPE_MAP` and `_ROLE_MAP`

Concrete drift symptoms: `.raquet` was cloud-native in code but matches no real
file (RaQuet rasters are `.parquet`; issue #487); `.webp`/`.gif` were documented
thumbnails but missing from the media-type/role maps, so those assets were typed
as `application/octet-stream` / role `data`; `.svg`, `.qix`, `.tfw`, `.gdb`,
`.tsv`, `.zarr`, `.copc.laz` were each recognized in some surfaces and absent
from others. Nothing failed a test when the surfaces disagreed.

The vocabulary is also the natural core of `reis`, the validator being extracted
from this repo (issue #563), so it is worth single-sourcing now.

## Decision

Introduce `portolan_cli/extension_registry.py` as the single source of truth: a
frozen-dataclass table (`EXTENSION_REGISTRY`) with one `ExtensionSpec` row per
extension, plus companion structures (`SIDECAR_OF`, `JUNK_DIRS`,
`STAC_FILENAMES`, `STYLE_FILENAMES`, `THUMBNAIL_MAX_SIZE`). Every frozenset and
map in the four code modules is **derived** from it via small helpers; the
public symbol names are unchanged, so consumers and tests are untouched.

The registry lives **inside the package**, not in `spec/`, because `spec/` is
not shipped in the wheel (the same reason `constants.PORTOLAN_SPEC_VERSION`
mirrors `spec/schema/spec-version.json`). It is a stdlib-only leaf that imports
nothing from `portolan_cli`, so — like `scan_classify`, `bbox`, and
`metadata.scan` (see the validation-seam contract in `pyproject.toml`) — it
moves cleanly into `reis` at extraction time.

`spec/extensions.md` remains the human doc and is tied to the registry by
`tests/spec_compliance/test_extensions_doc_parity.py`, which parses the markdown
tables and fails on any disagreement.

We chose a typed Python module over a shipped data file (YAML/JSON) because
`reis` is Python: the module extracts by moving, gives `mypy --strict` coverage
of every row, and needs no runtime parsing or `importlib.resources`.

## Consequences

- One place to add or reclassify an extension; the four modules and the doc stay
  in lock-step, enforced by the parity test.
- `.raquet` is dropped (issue #487); `.webp`/`.gif` are now correctly typed as
  thumbnails; `.svg` is a scan image; unsupported formats carry media types so
  companion assets stay well-typed; the doc gains `.gdb`, `.tsv`, `.zarr`,
  `.copc.laz`, `.qix`, `.tfw`.
- A new indirection: reading a frozenset now means reading a comprehension plus
  the registry row. Mitigated by keeping the derivations one line each and
  co-locating the rationale on each `ExtensionSpec` field.
- Out of scope (documented follow-up): `scan.py`'s `RECOGNIZED_*` sets and
  `scan.py`/`scan_fix.py`'s divergent `SHAPEFILE_*_SIDECARS` are additional drift
  not named by #558; folding them in would change sidecar-detection behavior.

## Alternatives considered

- **Reconcile values in place + a drift test, no registry.** Lighter, and the
  issue offered it as an option, but leaves the tables hand-written in four
  places and does not give `reis` a format module. Rejected in favor of a real
  single source.
- **Canonical machine-readable data file (YAML/JSON) in `spec/schema/`.** Matches
  the `rules.yaml` / `spec-version.json` precedent and is language-neutral, but
  `spec/` is not shipped at runtime and `reis` is Python, so the file would need
  a packaged copy or codegen. A typed module avoids both.
