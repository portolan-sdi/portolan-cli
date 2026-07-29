<!-- ops-sync:begin — synced from portolan-sdi/portolan-ops. Edit there, not here. -->

<!-- Title in conventional-commit form: it becomes the squash commit message. -->

## What this changes

<!-- Two or three sentences. What behaves differently once this merges. -->

## Why

<!-- One or two sentences. Link the issue rather than restating it. -->

## Verification

<!-- Paste the command you ran and the output you got. Name the data it ran
     against: a URL or a catalog path. Green tests are not verification. -->

- [ ] This change does not alter behavior (docs, chore, or CI only).

## Related issues

<!--
Budget: 200 words outside code blocks, no section longer than six lines. A
reviewer should finish this in under a minute. Prose follows the Portolan
voice: https://github.com/portolan-sdi/portolan-ops/blob/main/VOICE.md

CI checks the budget and the verification evidence on every push and edit.
-->

<!-- ops-sync:end -->

## Breaking Changes

<!-- What breaks, and how users migrate. Leave empty when nothing does. -->

## Checklist

See [what a finished PR looks like](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/contributing.md#what-a-finished-pr-looks-like).

- [ ] Tests written **first** and exercise real behavior (TDD)
- [ ] Integration coverage where the change crosses layers
- [ ] `prek run --all-files` is green locally; all required CI checks pass
- [ ] Changed lines are covered (`codecov/patch`)
- [ ] At least one adversarial review (actively tried to break it)
- [ ] CodeRabbit comments addressed
- [ ] Docs updated and, for non-obvious decisions, an ADR added
