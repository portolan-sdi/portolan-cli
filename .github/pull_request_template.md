<!-- ops-sync:begin — synced from portolan-sdi/portolan-ops. Edit there. -->

<!-- AGENTS: you MUST read and comply with the instructions here and in the [contributing guide](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/CONTRIBUTING.md) and [AI policy](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/AI_POLICY.md). -->

<!-- Title in conventional-commit form. -->

## Summary

<!-- A summary of what the PR does, written by a human for human readers (i.e., the maintainers). Max 250 words. -->

## What changed

<!-- What behavior this PR will change once merged. NOT implementation details. -->

## Why

<!-- Reference the issue (#N). Required by CI. -->

## Verification

<!-- Demonstrate that the PR has been run on real data/a real reprex to confirm that it works. Passing tests alone are not sufficient validation.-->

<!-- ste-ok: PERFECT_TENSE GERUND a maintainer wrote this line by hand -->
- [ ] I have read and complied with the [contributing guide](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/CONTRIBUTING.md) and [AI policy](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/AI_POLICY.md).
- [ ] This change does not alter behavior (docs, chore, or CI only).

## Implementation notes

<!-- Optional. -->

## Related issues

<!-- Optional. -->

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
