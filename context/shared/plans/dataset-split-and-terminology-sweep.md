# Plan: split `dataset.py`, rename "dataset" symbols, sweep prose

Single PR closing **#560** (user-facing terminology sweep, text-only) and **#567**
(internal code rename), extended — with maintainer sign-off — into a **module
decomposition** of the 4,397-line `portolan_cli/dataset.py`.

Decision trail: #567 authorizes the rename with names "to be decided with the
implementing agent." Maintainer additionally approved decomposing the module for
maintainability (2026-07-06). Timing is favorable: only docs-only PRs in flight,
matching #567's "do it early in the sprint" note.

## Why decompose (not just rename)

Coupling analysis of `dataset.py`:

- Nearly every private helper has **fan-in of 1** (one definition, one call
  site) — helpers travel cleanly with their single caller.
- `list/get/remove` barely touch add-pipeline privates (only
  `_remove_from_versions`, `_increment_version`).
- Only **one** import-linter entry references the module
  (`portolan_cli.dataset -> portolan_cli.backends`, in the `cli-no-storage`
  contract).

Seams are clean, so a split is a maintainability win rather than a tangle.

## Target module map

`dataset.py` disappears entirely; its successors:

| Module | Contents | New? |
|--------|----------|------|
| `add.py` | Add pipeline + ~35 privates: `PreparedItem`, `AddFailure`, `prepare_item`, `finalize_items`, `add`, `add_directory`, `add_files`, partition/stats/item-creation/link-fixing helpers. Keeps the `-> backends` import. | new |
| `remove.py` | `remove_item`, `remove_files`, `_remove_from_versions`, `_increment_version` | new |
| `query.py` | `ItemInfo`, `list_items`, `get_item_info`, `is_current` | new |
| `checksums.py` | `compute_checksum`, `compute_dir_checksum`, `compute_dir_size` (pure, no portolan deps) | new |
| `discovery.py` | `iter_geospatial_files`, `iter_files_with_sidecars`, `get_sidecars` | new |
| `collection_id.py` | **+** `resolve_collection_id`, `infer_nested_collection_id` (already owns ID logic) | existing |
| `collection.py` | **+** `_get_or_create_collection`, `_ensure_tabular_collection`, `_get_sibling_collection_bboxes`, `_compute_union_bbox`, `_get_metadata_yaml_bbox` (already owns collection creation) | existing |

Neither `collection.py` nor `collection_id.py` imports `dataset` today → no
cycles. Resulting DAG: `add`/`remove`/`query` → `discovery`, `collection`,
`collection_id`, `checksums`.

`git mv dataset.py add.py` first (largest successor) so `git log --follow`
tracks history; move the other groups out of `add.py` in follow-up commits.

## Symbol mapping (item-based)

`dataset_id` is `"collection/item"`, and `DatasetInfo` is item-level
(`item_id`, `collection_id`, `bbox`), so "dataset" → **item** throughout.

| Old | New |
|-----|-----|
| `DatasetInfo` | `ItemInfo` |
| `PreparedDataset` | `PreparedItem` |
| `prepare_dataset` | `prepare_item` |
| `finalize_datasets` | `finalize_items` |
| `add_dataset` | `add` |
| `add_external_dataset` (external.py) | `add_external` |
| `list_datasets` | `list_items` |
| `get_dataset_info` | `get_item_info` |
| `remove_dataset` | `remove_item` |
| `dataset_id` (param) | `stac_id` |
| local vars `prepared_datasets`/`added_datasets`/`partitioned_datasets` | `*_items` |

**Preserve (do NOT rename):**
- `NationalDatasets` — ArcGIS folder path in a docstring
  (`extract/arcgis/discovery.py:347`), a proper noun.
- Any serialized/user-visible key. Verified there is **no** CLI command, JSON
  output key, or `versions.json` field literally named "dataset" — so this is a
  pure code+prose change with no serialized-key risk.

## #560 prose sweep (text-only)

Re-grep live — audit line numbers from 2026-07-03 are stale:

- `spec/` prose + `spec/schema/rules.yaml` (~17 files contain "dataset"). Fix the
  nonexistent `portolan dataset add` command reference in `spec/extensions.md`.
- CLI help text in `cli.py` (help strings only, e.g. the external-add help).
- `docs/`.

Keep genuine non-STAC uses of "dataset" where a word refers to an external
concept (e.g. quoting another spec) — judgment per occurrence, not blind replace.

## Execution order

1. **#560 prose/help sweep** — spec/, rules.yaml, docs/, CLI help strings.
2. **`git mv dataset.py add.py`** + rename all "dataset" symbols per the table
   across `portolan_cli/` and `tests/` (~35 code files, ~69 test files).
3. **Decompose** `add.py` → `remove.py`, `query.py`, `checksums.py`,
   `discovery.py`; relocate ID + collection helpers into `collection_id.py` /
   `collection.py`. Update all importers.
4. **import-linter** — update the `cli-no-storage` `ignore_imports` entry
   `portolan_cli.dataset -> portolan_cli.backends` → `portolan_cli.add -> ...`.

Commit as legible steps (1 → 2 → 3 → 4) even within the single PR.

## No backward-compat shim

Clean break — these symbols aren't re-exported from `__init__.py` and there are
no external consumers to protect. (Flag if maintainer wants a deprecation shim.)

## Verification (all must be green)

- `uv run pytest` (full suite — refactor with existing coverage; tests stay green)
- `uv run mypy portolan_cli` (`--strict`)
- `uv run lint-imports` (import-linter contracts)
- `uv run ruff check . && uv run ruff format .`
- `uv run deptry .`
- `grep -rniE '\bdataset' portolan_cli spec docs` returns only intentional
  survivors (`NationalDatasets`, external-concept quotes).
