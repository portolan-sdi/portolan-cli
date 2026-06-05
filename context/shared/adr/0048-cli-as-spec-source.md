# ADR-0048: CLI Repository as Specification Source of Truth

## Status
Accepted

## Context

The Portolan specification was originally maintained in a separate repository (`portolan-spec`) from the CLI implementation (`portolan-cli`). This created several friction points:

1. **Sync overhead**: Changes to the spec required PRs in one repo, then corresponding implementation PRs in another
2. **Proposal process complexity**: The spec repo had PROPOSALS.md, QUESTIONS.md, and a multi-step process that slowed iteration
3. **Context-switching**: Contributors had to work across repos for related changes
4. **Drift risk**: Spec and implementation could diverge without automated validation

The CLI-first workflow establishes a core principle: **the spec documents what the CLI does, not what it might do**. This inverts the traditional standards-first approach in favor of working software.

### Forces at play

1. **Standards bodies**: External references need a clean, standalone spec URL
2. **Contribution simplicity**: One repo, one PR is easier than coordinating across repos
3. **Atomic changes**: Implementation + spec changes should land together
4. **History preservation**: Git history has value, but not enough to justify complex migration

## Decision

### 1. CLI repo is the source of truth

All spec content lives in `portolan-cli/spec/`. This includes:
- `spec/schema/` — JSON schemas for catalog, collection, versions
- `spec/README.md` — Spec overview and contribution guide

### 2. Spec repo becomes a read-only mirror

The `portolan-spec` repository remains accessible for standards body references but:
- Receives updates only via CI sync from `portolan-cli/spec/`
- Has issues disabled
- Has direct PRs rejected with a redirect to portolan-cli
- Preserves CODE_OF_CONDUCT.md (static, not synced)

### 3. PRs are proposals

The separate PROPOSALS.md process is eliminated. A PR that modifies `spec/` IS the proposal. Discussion happens in the PR, and merge means acceptance.

### 4. Git history is not preserved

The migration does not preserve commit history from portolan-spec. The complexity of `git filter-branch` or subtree merges isn't worth it for this scale. The spec repo history remains accessible for archaeology.

## Consequences

### Positive
- Single source of truth eliminates drift
- Atomic PRs enable implementation + spec changes together
- Simpler contribution model (one repo)
- CI validates spec changes against implementation immediately

### Negative
- Spec repo loses direct contribution path (mitigated by clear redirect)
- `spec/` directory adds to CLI repo size (minimal impact)

### Neutral
- Standards bodies still reference portolan-spec URL (unchanged)
- Sync workflow adds CI complexity (acceptable)

## Implementation

1. Create `spec/` directory in portolan-cli with schema files
2. Update test imports to reference `spec/schema/`
3. Rename `sync-schemas-to-spec.yml` → `sync-spec.yml`
4. Update sync workflow to copy entire `spec/` directory
5. Update portolan-spec README with mirror notice
6. Disable issues in portolan-spec

## Alternatives Considered

| Alternative | Why Rejected |
|-------------|--------------|
| Keep separate repos with manual sync | Maintenance burden, drift risk |
| Merge repos entirely (delete spec repo) | Spec repo needed for standards body references |
| Spec repo as source, CLI pulls from it | Violates CLI-first principle |
| Preserve git history in migration | Complexity not worth it for this scale |
