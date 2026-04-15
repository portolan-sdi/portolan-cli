# ADR-0041: Iceberg as Optional Extra, Not Separate Package

## Status

Accepted (supersedes [ADR-0004](0004-iceberg-as-plugin.md))

## Context

ADR-0004 established Iceberg as a separate plugin package (portolake), registered via the `portolan.backends` entry point. After building portolake to v0.1.0, we found that maintaining a separate repository creates significant friction:

**Evidence of coupling:** portolake imports from portolan-cli at 6 sites, but only 1 via the declared plugin protocol. The other 5 reach into internal modules (versions, upload, download, pull, output).

**Recurring coordination pain:** Every protocol change requires:
1. Modify portolan-cli
2. Wait for PyPI release
3. Update portolake's dependency pin
4. Unblock portolake CI

This pattern recurred with every new protocol method. As a workaround, portolake pinned to `git+main`, which is itself fragile.

**Single plugin:** The "pluggable ecosystem" ADR-0004 anticipated never materialized. portolake is the only backend plugin.

GitHub issue [#341](https://github.com/portolan-sdi/portolan-cli/issues/341) analyzed four options. Option D (single package with optional extras) was selected by collaborator consensus.

## Decision

Merge portolake into portolan-cli as `pip install portolan-cli[iceberg]`.

- Code lives at `portolan_cli/backends/iceberg/`
- Dependencies gated behind `[iceberg]` optional extra: pyiceberg[sql-sqlite], shapely, pygeohash
- Default install unchanged — no pyiceberg, no extra weight
- `get_backend("iceberg")` uses lazy import with clear error if extra not installed
- Python 3.11+ required for iceberg features (enforced by pyiceberg dependency)
- `import-linter` contract enforces `portolan_cli.backends.iceberg` cannot import `portolan_cli.cli`

The architectural separation from ADR-0004 survives: Iceberg is not forced on simple users. Only the packaging changes — from separate repo to optional extra.

## Consequences

### What becomes easier

- **Atomic protocol changes** — one PR instead of cross-repo coordination
- **Simpler installation** — `pip install portolan-cli[iceberg]` instead of separate `pip install portolake`
- **Unified CI** — tests run together; integration tests can test both backends
- **Import honesty** — the 5 internal imports are now legitimate intra-package references
- **Single version** — no more version-pinning dance between repos

### What becomes harder

- **pyproject.toml complexity** — optional deps, mypy overrides, vulture excludes
- **CI path filtering** — needed to avoid running iceberg tests on unrelated changes
- **ADR-0004 supersession** — this ADR must clearly document the reasoning

### Trade-offs

- We accept modest pyproject.toml complexity for development simplicity
- We accept CI path filtering overhead for unified test coverage

## Alternatives Considered

### A. Keep separate repos (status quo)

**Rejected:** Cross-repo friction outweighs separation benefits for a 1K LOC plugin with deep coupling.

### B. Extract protocol to standalone package

**Rejected:** Third repo to maintain; the protocol rarely changes, but the internal imports (5 of 6) would still require coordination.

### C. Monorepo with separate packages (uv workspace)

**Rejected:** Overcomplicated build; two packages from one repo is unusual and adds release complexity for no practical benefit over option D.

### D. Single package with `[iceberg]` extra (chosen)

**Selected:** Maximum dev velocity. The ~50 MB dependency delta (pyiceberg + transitive) is modest — roughly 3% of the existing venv footprint, smaller than pandas alone.
