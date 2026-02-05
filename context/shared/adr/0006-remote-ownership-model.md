# ADR-0006: Remote Ownership Model

## Status
Accepted

## Context

Portolan syncs local catalogs to remote object storage (S3, GCS, Azure, etc.). A key question: what happens if someone modifies files in the bucket outside of Portolan?

Options range from "merge external changes" to "ignore everything outside Portolan" to "fail loudly."

## Decision

**Portolan owns the bucket contents.**

The principle: *If it's in the bucket, Portolan put it there. If Portolan didn't put it there, it's a problem.*

### What this means

1. **Users configure access** — Credentials, endpoint, bucket path
2. **Portolan manages everything inside** — All files, all structure
3. **Manual edits are unsupported** — Will be flagged as drift
4. **Drift detection is built-in** — `portolan check --remote` compares actual state vs expected
5. **Repair is authoritative** — `portolan repair` forces remote to match local

### Drift scenarios

| Scenario | Detection | Resolution |
|----------|-----------|------------|
| File added outside Portolan | Flagged as unexpected | User removes or Portolan ignores |
| File deleted outside Portolan | Flagged as missing | `portolan repair` re-uploads |
| File modified outside Portolan | Checksum mismatch | `portolan repair` overwrites |
| versions.json modified | Checksum mismatch | `portolan repair` overwrites |

### Not a merge model

Portolan does not attempt to merge external changes. If remote diverges from local, the options are:
1. **Accept local as truth** — `portolan repair`
2. **Accept remote as truth** — Manual: delete local, re-pull
3. **Investigate** — `portolan check --remote` shows what differs

## Consequences

### What becomes easier
- **Consistency guarantees** — Remote always matches what Portolan expects
- **Simple mental model** — One source of truth (local), one sync direction
- **Corruption recovery** — Re-run repair to restore known-good state
- **Caching/CDN safety** — Files don't change unexpectedly

### What becomes harder
- **Multi-tool workflows** — Can't use other tools to modify the bucket
- **Migration** — Existing buckets need to be "adopted" by Portolan
- **Collaboration edge cases** — Two users with different local states

### Trade-offs
- We accept reduced flexibility for increased reliability
- We accept "Portolan or nothing" for the bucket in exchange for strong consistency

## Alternatives Considered

### 1. Merge model (detect and incorporate external changes)
**Rejected:** Complexity explosion. What if external change conflicts with local? What if external file has no metadata? Opens endless edge cases.

### 2. Ignore model (don't check remote state)
**Rejected:** Silent corruption. Users wouldn't know if files were tampered with or accidentally deleted.

### 3. Read-only remote (Portolan only reads, external tool writes)
**Rejected:** Inverts the use case. Portolan is for publishing, not consuming.

### 4. Shared ownership with locking
**Considered for multi-user:** See issue #18. Locking allows multiple Portolan instances to share a bucket safely. External (non-Portolan) modifications remain unsupported.
