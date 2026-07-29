---
paths:
  - "docs/**"
  - "context/**"
  - "**/*.md"
---

# Documentation rules

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
- **`context/`**, Internal, AI-oriented context (maintainer rationale, known issues, CI and tooling notes). Dense, structured, co-located with development. NOT published.

Do NOT put architectural plans or design documents in `docs/`. Working plans are
local scratch and are git-ignored; once a decision holds, record it in one of the
four destinations below rather than leaving it in a plan.

## Documentation Bias

**Bias toward documenting everything.** AI agents work best with rich context.

### What to Document

| What | Where | When |
|------|-------|------|
| User-facing behavior and its reasoning | `docs/` | Anything an operator needs to predict what the CLI does |
| Maintainer rationale | `context/shared/documentation/` | Why the code is shaped this way, rejected alternatives |
| Known bugs/issues | `context/shared/known-issues/` | When a bug or environment constraint is identified but not fixed |
| Non-obvious code | Inline comments | Code that would confuse a future reader |
| API contracts, gotchas at the call site | Docstrings | All public functions/classes |
| Catalog conformance rules | portolan-spec repo | Anything normative about catalog shape |

### Record decisions next to what they govern

This repo has no ADR directory. It had one, and the ADRs drifted: several
described modules and CLI flags that were never built, and others restated rules
the spec now owns. A decision record that outlives its subject is worse than no
record, because it reads as authoritative.

Write the decision where someone will hit it while changing the thing:

- **A rule about catalog shape** belongs in portolan-spec, enforced by a rashid
  `PTL-*` rule. Not here.
- **A rule about how the CLI behaves** belongs in `docs/`, next to the setting or
  command it governs.
- **A constraint you accepted and did not fix** belongs in
  `context/shared/known-issues/`, stated as a limitation with its workaround.
- **A trade-off or rejected alternative** belongs in
  `context/shared/documentation/` when it spans modules, or in the module's
  docstring when it does not.

Prefer the docstring. A rationale attached to the function it explains cannot
drift away from it.

### Two Documentation Audiences

| Audience | Location | Purpose |
|----------|----------|---------|
| **Humans** | `docs/` (mkdocs) | *How to use*, tutorials, visual guides |
| **AI agents** | Docstrings, AGENTS.md, `.claude/rules/` | *How to modify*, dense, structured, co-located with code |

### Validating AI Guidance

**When possible, back AI guidance with automated validation.** Documentation drifts, code doesn't lie.

If AGENTS.md names a command, enforce that the command exists with a script. If it says "use `output.py` for terminal messages," add a lint rule. The goal: make it impossible for guidance to become stale.

**Pattern:**
1. Write guidance in AGENTS.md
2. Ask: "Can I validate this automatically?"
3. If yes, write a script in `scripts/` and add a pre-commit hook

**Example:** The command surface in the root `AGENTS.md` is validated by `scripts/validate_agents_md.py`, which re-reads the Click decorators in `cli.py`. Commits that rename a command without updating AGENTS.md are blocked.

When adding new guidance, consider: can this be validated? If so, add a check.
