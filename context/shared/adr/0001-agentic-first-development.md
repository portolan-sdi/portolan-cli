# ADR-0001: Agentic-First Development Strategy

## Status
Accepted

## Context

The core contributors to portolan-cli will primarily use agentic coding tools (Claude Code, Cursor, etc.) for development. This creates a fundamental shift in how code is produced:

- **Volume exceeds review capacity** — AI agents can generate code faster than humans can manually review it
- **Consistency is harder to enforce** — Different agents may produce stylistically inconsistent code
- **Hallucinations are a risk** — AI-generated tests may be tautological (testing what's implemented rather than what's correct)
- **Quality gates must scale** — Manual code review becomes a bottleneck

Traditional development workflows assume human-paced code production with human review. That model doesn't scale to agentic development.

## Decision

We adopt an **agentic-first development strategy** with the following principles:

### 1. Automate All Quality Gates

Every quality check must be automated and enforced in CI. No exceptions, no manual steps.

| Layer | Tools | Purpose |
|-------|-------|---------|
| Formatting | ruff format | Consistent style |
| Linting | ruff check | Code quality |
| Types | mypy (strict) | Type safety |
| Dead code | vulture | Remove unused code |
| Complexity | xenon | Prevent spaghetti |
| Security | bandit, pip-audit | Vulnerability scanning |
| Tests | pytest | Correctness |
| Mutation | mutmut | Test quality |
| Property | hypothesis | Invariant verification |

### 2. Test-Driven Development (Mandatory)

All features must be implemented test-first:

1. Write failing tests
2. Run tests (verify failure)
3. Implement minimal code to pass
4. Run tests (verify success)
5. Refactor if needed

This creates a forcing function that prevents agents from writing untested code.

### 3. Defend Against Tautological Tests

AI agents may write tests that verify implementation rather than behavior. We counter this with:

- **Mutation testing (mutmut)** — If tests don't catch mutations, they're not testing anything
- **Property-based testing (hypothesis)** — Define invariants that must hold for any input
- **Human-written test specs** — `tests/specs/` contains human-defined behaviors; agents implement the tests

### 4. AI-Augmented Review

Use AI tools to provide additional review layers:

- **CodeRabbit** — Automated PR review for patterns and issues
- **Separate review agents** — Draft PRs, then review with a fresh agent context
- **Iterative refinement** — Use agent feedback to improve before human review

### 5. Start Clean, Stay Clean

Impose strict quality standards from day one:

- All checks are blocking (no warnings, no `continue-on-error`)
- No technical debt "to fix later"
- Pre-commit hooks enforce standards before code enters the repo
- CI enforces standards before code enters main

### 6. Document Everything

Maximize context for AI agents:

- ADRs for architectural decisions
- `context/shared/known-issues/` for tracked bugs
- Inline comments for non-obvious code
- CLAUDE.md as the source of truth for development patterns

## Consequences

### What becomes easier

- **Scaling development** — More contributors (human or AI) without review bottleneck
- **Consistency** — Automated tools enforce uniform standards
- **Onboarding** — New agents have clear context via CLAUDE.md and ADRs
- **Confidence** — Comprehensive automated checks catch issues early

### What becomes harder

- **Initial setup** — More tooling configuration upfront
- **Strict enforcement** — No shortcuts, even for "simple" changes
- **Learning curve** — Contributors must understand the workflow
- **False positives** — Strict tools may flag acceptable patterns

### Trade-offs

- We accept slower initial velocity for sustainable long-term velocity
- We accept more tooling complexity for higher code quality
- We accept stricter constraints for reduced manual review burden

## Alternatives Considered

### 1. Traditional human-centric review
**Rejected:** Doesn't scale with agentic code generation volume.

### 2. Lenient initial standards, tighten later
**Rejected:** Technical debt compounds; cleaning up is harder than starting clean.

### 3. Manual mutation testing
**Rejected:** Humans won't consistently run mutation tests; must be automated.

### 4. Skip property-based testing
**Rejected:** Example-based tests alone can't verify invariants across input space.
