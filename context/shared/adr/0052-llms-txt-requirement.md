# ADR-0052: Require AGENTS.md for AI/LLM Integration

**Date**: 2026-06-05 (revised 2026-07-09: `llms.txt` → `AGENTS.md`)
**Status**: Accepted

## Decision

Every Portolan catalog and collection **MUST** include an `AGENTS.md` file linked via `rel: "agents"` in the STAC JSON.

## Context

AI agents and LLMs are increasingly used to discover, query, and analyze geospatial data. Machine-readable STAC metadata alone is not enough — agents need plain-language context about what a dataset contains, how to query it, what the fields mean, and what pitfalls to avoid.

Portolan originally adopted the [llms.txt](https://llmstxt.org/) convention for this role. As of 2026-07-09 it standardizes on [`AGENTS.md`](https://agents.md/) instead: an emerging, cross-tool Markdown convention (the same file agentic coding tools already look for), which is a better fit for a human-authored, Markdown guide and avoids inventing a Portolan-specific filename. This supersedes the earlier `llms.txt` direction; the `rel` relation moves from `"llms"` to `"agents"` and the filename from `llms.txt` to `AGENTS.md`.

## Rationale

- AI agents are a primary audience for cloud-native geodata catalogs
- STAC metadata describes structure but not semantics — agents need both
- Markdown is the most natural format for LLMs to consume
- `AGENTS.md` is co-located with the data, versioned alongside it, and aligns with a convention agents already recognize
- Content is human-authored and open-ended: publishers add whatever helps an agent use the data (joins, sample queries, data-quality notes, related collections) — things not already in the README

## Consequences

- All catalogs and collections must maintain an `AGENTS.md` file
- The file must be linked in STAC JSON with `rel: "agents"` and `type: "text/markdown"`, using a relative `href`
- `AGENTS.md` is a **link**, not an asset
- Content recommendations are provided but expected to evolve rapidly as best practices emerge
- Enforcement (implemented, not just specified):
  - The shipped JSON schemas (`spec/schema/{catalog,collection}.schema.json`) **require** at least one `rel="agents"` link (via `contains`), not merely validate its shape when present
  - Error-level `RULE-0080` (catalog) and `RULE-0081` (collection) in `spec/schema/rules.yaml` are backed by CLI validators (`CatalogAgentsMdLinkRule` / `CollectionAgentsMdLinkRule` in `portolan_cli/validation/rules.py`); they flag a missing `AGENTS.md` file or link
  - `portolan init` and `portolan add` scaffold `AGENTS.md` and emit the link, so freshly-created catalogs are compliant without a follow-up step; `portolan check --fix` (`repair_agents_md`) backfills the file and link for externally-modified catalogs. Scaffolding never overwrites an existing, human-authored `AGENTS.md`
  - Warning-level `RULE-0082/0083` document recommended content and remain spec-level guidance (not code-enforced, since content is open-ended)

## References

- [ai-integration.md](../../../spec/ai-integration.md) — Full specification
- [agents.md](https://agents.md/) — The AGENTS.md convention
- [llmstxt.org](https://llmstxt.org/) — The superseded llms.txt standard
