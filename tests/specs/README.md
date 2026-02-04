# Test Specifications

Human-written test specifications that define **what** should be tested, not **how**.

## Purpose

AI agents write most test implementations, but humans define what matters. This directory contains specifications that:

1. **Define expected behaviors** — What the system should do
2. **Specify edge cases** — Boundary conditions that must be handled
3. **Document invariants** — Properties that must always hold
4. **Prevent tautological tests** — Ensure tests actually verify something meaningful

## Format

Each spec file should follow this structure:

```markdown
# Feature: [Name]

## Happy Path
- [ ] Given X, when Y, then Z
- [ ] ...

## Edge Cases
- [ ] Empty input returns ...
- [ ] Invalid input raises ...

## Invariants
- [ ] Output always satisfies ...
- [ ] State is never ...
```

## Workflow

1. **Human writes spec** — Define what matters in a `.md` file
2. **AI implements tests** — Generate pytest code from the spec
3. **Mutation testing verifies** — Nightly `mutmut` runs confirm tests catch real bugs
4. **Spec evolves** — Update specs as requirements change

## Files

(Add spec files here as features are developed)
