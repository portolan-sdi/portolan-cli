<!-- ops-sync:begin — synced from portolan-sdi/portolan-ops. Edit there, not here. -->

<!-- Title in conventional-commit form: it becomes the squash commit message. -->

## What changed

<!-- What behaves differently once this merges. Write the outcome, not the
     work you did to reach it. Skip "first I tried X, then Y". -->

## Why

<!-- Reference the issue (#N) rather than restating it. CI requires the
     reference unless the waiver below is ticked. -->

## Verification

<!-- Re-run the reproduction from the linked issue. Paste the command and its
     output in a fenced block, and name the data it read: a URL or a catalog
     path. Show the reported behavior changed. Green tests are not
     verification. -->

<!-- Keep the checkbox wording intact: CI matches its exact phrase. Tick at
     most one. -->

- [ ] This change does not alter behavior (docs, chore, or CI only).
- [ ] This pull request integrates changes already verified in their own pull
      requests (a release or integration branch). List them under "What
      changed".

## Implementation notes

<!-- Optional. Files touched, design choices, edge cases, and anything a
     reviewer needs that does not belong in the opening. Length is fine
     here. -->

## Related issues

<!--
Write for two readers. A human reads the top and learns what changed, why it
matters, and that it works. A reviewer or agent reads on for evidence and
detail. There is no word limit. A long body is good when its opening makes
the outcome obvious.

Use short sentences and plain words. A hook checks the body when you run
`gh pr create`. See `.claude/hooks/writing_check.py --print-rules`.
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
