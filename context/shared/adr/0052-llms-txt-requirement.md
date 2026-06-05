# ADR-0052: Require llms.txt for AI/LLM Integration

**Date**: 2026-06-05
**Status**: Accepted

## Decision

Every Portolan catalog and collection **MUST** include an `llms.txt` file linked via `rel: "llms"` in the STAC JSON.

## Context

AI agents and LLMs are increasingly used to discover, query, and analyze geospatial data. Machine-readable STAC metadata alone is not enough — agents need plain-language context about what a dataset contains, how to query it, what the fields mean, and what pitfalls to avoid. The [llms.txt](https://llmstxt.org/) standard provides a convention for LLM-friendly documentation.

## Rationale

- AI agents are a primary audience for cloud-native geodata catalogs
- STAC metadata describes structure but not semantics — agents need both
- Markdown is the most natural format for LLMs to consume
- The llms.txt convention is simple, co-located with the data, and versioned alongside it
- Early experience with [portolan-nl](https://source.coop/cholmes/portolan-nl) shows this pattern works well in practice

## Consequences

- All catalogs and collections must maintain an `llms.txt` file
- The file must be linked in STAC JSON with `rel: "llms"` and `type: "text/markdown"`
- Content recommendations are provided but expected to evolve rapidly as best practices emerge
- Validation rules added:
  - RULE-0080/0081: error-level rules requiring llms.txt link at catalog and collection levels
  - RULE-0082/0083: warning-level rules for recommended content in llms.txt files

## References

- [ai-integration.md](../../../spec/ai-integration.md) — Full specification
- [llmstxt.org](https://llmstxt.org/) — The llms.txt standard
- [portolan-nl](https://source.coop/cholmes/portolan-nl) — Example implementation on Source Cooperative
