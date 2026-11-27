---
paths:
  - "docs/**"
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

## docs/ vs code-adjacent notes

- **`docs/`**, Public-facing, human-readable documentation (tutorials, visual guides, user-oriented). Built with Zensical and published.
- **Internal rationale** lives in the commit body and the PR body. Read it with
  `git log` for the file. There is no separate notes directory.

Do NOT put architectural plans or design documents in `docs/`. Working plans are
local scratch and are not committed. Once a decision holds, record it in one of
the destinations below rather than leaving it in a plan.

### Where a decision goes

| What | Where |
|------|-------|
| User-facing behavior and its reasoning | `docs/`, next to the setting or command it governs |
| Catalog conformance rules | portolan-spec, enforced by a rashid `PTL-*` rule |
| Known upstream bugs and accepted limits | A one-line comment with the upstream URL, or a `skipif` or `xfail(strict=True)` reason, at the workaround |
| Trade-offs, rejected alternatives, CI decisions | The commit body and the PR body |

Docstrings and comments follow the 2-line rule in `.claude/rules/python.md`.
A strict `xfail` fails CI when upstream fixes the bug, so the record cannot
outlive the bug.

### Two Documentation Audiences

| Audience | Location | Purpose |
|----------|----------|---------|
| **Humans** | `docs/` (Zensical) | *How to use*, tutorials, visual guides |
| **AI agents** | AGENTS.md, `.claude/rules/` | *How to modify*, dense, structured, co-located with code |

### Validating AI Guidance

**When possible, back AI guidance with automated validation.** Documentation drifts, code doesn't lie.

If AGENTS.md names a command, enforce that the command exists with a script. If it says "use `output.py` for terminal messages," add a lint rule. The goal: make it impossible for guidance to become stale.

**Pattern:**
1. Write guidance in AGENTS.md
2. Ask: "Can I validate this automatically?"
3. If yes, write a script in `scripts/` and add a prek hook

**Example:** The command surface in the root `AGENTS.md` is validated by `scripts/validate_agents_md.py`, which re-reads the Click decorators in `cli.py`. Commits that rename a command without updating AGENTS.md are blocked.

When adding new guidance, consider: can this be validated? If so, add a check.
