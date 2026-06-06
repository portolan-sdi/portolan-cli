---
paths:
  - "docs/**"
  - "context/**"
  - "spec/**"
  - "**/*.md"
---

# Documentation and ADR rules

## Documentation Accuracy (CRITICAL)

**GitHub Issues + Milestones are the source of truth for planned vs implemented features.**

When documenting CLI commands:
1. **Run `portolan <command> --help`** to verify actual behavior
2. **Check [GitHub Issues](https://github.com/portolan-sdi/portolan-cli/issues?q=label%3Aroadmap%3Amvp)** for planned features
3. **Do NOT deprecate planned features**, if it's in GitHub Issues as planned, it's intended
4. **Do NOT simplify orchestration commands**, document the FULL workflow

**Example:** `portolan sync` orchestrates `pull → init → scan → check → push`. Do NOT describe it as just "pull + push", that misrepresents the command's purpose.

## docs/ vs context/ distinction

- **`docs/`**, Public-facing, human-readable documentation (tutorials, visual guides, user-oriented). Built with mkdocs and published.
- **`context/`**, Internal, AI-oriented context (architectural plans, design docs, ADRs, research). Dense, structured, co-located with development. NOT published.

Do NOT put architectural plans or design documents in `docs/`. Those belong in `context/shared/plans/`.

## Documentation Bias

**Bias toward documenting everything.** AI agents work best with rich context.

### What to Document

| What | Where | When |
|------|-------|------|
| Architectural decisions | `context/shared/adr/` | Any non-obvious design choice |
| Known bugs/issues | `context/shared/known-issues/` | When a bug is identified but not yet fixed |
| Non-obvious code | Inline comments | Code that would confuse a future reader |
| API contracts | Docstrings | All public functions/classes |
| Gotchas/quirks | CLAUDE.md or inline | Anything that surprised you |

### ADR Guidelines

Create an ADR (`context/shared/adr/NNNN-title.md`) when:

- Choosing between multiple valid approaches
- Adopting a new dependency
- Establishing a pattern that others should follow
- Making a trade-off that isn't obvious

Use the template at `context/shared/adr/0000-template.md`. Every new ADR must be
added to the ADR Index in the root `CLAUDE.md` (enforced by
`scripts/validate_claude_md.py`).

### Two Documentation Audiences

| Audience | Location | Purpose |
|----------|----------|---------|
| **Humans** | `docs/` (mkdocs) | *How to use*, tutorials, visual guides |
| **AI agents** | Docstrings, CLAUDE.md, ADRs | *How to modify*, dense, structured, co-located with code |

### Validating AI Guidance

**When possible, back AI guidance with automated validation.** Documentation drifts, code doesn't lie.

If CLAUDE.md says "all ADRs must be listed in the index," enforce it with a script. If it says "use `output.py` for terminal messages," add a lint rule. The goal: make it impossible for guidance to become stale.

**Pattern:**
1. Write guidance in CLAUDE.md
2. Ask: "Can I validate this automatically?"
3. If yes, write a script in `scripts/` and add a pre-commit hook

**Example:** The ADR index in the root `CLAUDE.md` is validated by `scripts/validate_claude_md.py`, which checks that all ADRs in `context/shared/adr/` are listed. This runs as a pre-commit hook, commits that add ADRs without updating CLAUDE.md are blocked.

When adding new guidance, consider: can this be validated? If so, add a check.
