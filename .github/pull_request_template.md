<!-- ops-sync:begin — synced from portolan-sdi/portolan-ops. Edit there, not here. -->

<!-- Title in conventional-commit form: it becomes the squash commit message. -->

## What this changes

<!-- Two or three sentences. What behaves differently once this merges. -->

## Why

<!-- One or two sentences. Reference the issue (#N) rather than restating
     it. CI requires the reference unless the waiver below is ticked. -->

## Verification

<!-- Re-run the reproduction from the linked issue and paste the command
     and output here, in a fenced block. Name the data it read: a URL or a
     catalog path. Show the reported behavior changed, not that a command
     runs. Green tests are not verification. -->

<!-- Keep the checkbox wording intact: CI matches its exact phrase. -->

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
