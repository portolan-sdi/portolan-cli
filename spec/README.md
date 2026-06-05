# Portolan Specification

This directory contains the canonical Portolan specification.

The **portolan-cli repository is the source of truth** for the spec. The
[portolan-spec](https://github.com/portolan-sdi/portolan-spec) repository is a
read-only mirror, automatically synced via CI on every merge to main.

## Contents

- `schema/` — Machine-readable JSON schemas and validation rules

## Making Changes

To propose spec changes:

1. Open a PR in this repository (portolan-cli)
2. Changes to `spec/` trigger review from spec maintainers
3. On merge, CI syncs to portolan-spec automatically

See [ADR-0048](../context/shared/adr/0048-cli-as-spec-source.md) for the
rationale behind this structure.

## Why CLI-First?

The spec documents what the CLI does, not what it might do. Keeping spec and
implementation in the same repo enables:

- **Atomic PRs** — Implementation + spec changes land together
- **Single source of truth** — No sync drift between repos
- **Simpler contribution model** — One repo, one PR
