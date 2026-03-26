# ADR-0037: Experimental Extension Policy

## Status
Accepted

## Context
STAC extensions have maturity levels: Proposal → Pilot → Stable. Vector extension is v0.1.0 (Proposal). Should we use unstable extensions or wait?

## Decision
1. **Use experimental extensions** — don't wait for stability
2. **Accept migration cost** — when extensions change, update Portolan
3. **No fallback prefixes** — use `vector:geometry_types`, not `portolan:geometry_types`
4. **Document maturity** in generated STAC (already required by spec: `stac_extensions` array)
5. **Portolan may contribute upstream** — we'll likely help stabilize Vector extension

## Consequences
- Users get features immediately
- Breaking changes in extensions require Portolan updates
- No tech debt from custom prefixes that would need migration later
- Portolan catalogs are interoperable with other STAC tools from day one

## Alternatives considered
- **Wait for stable:** Rejected — could be years; blocks useful features
- **Custom `portolan:` prefix:** Rejected — creates migration burden when extension stabilizes
