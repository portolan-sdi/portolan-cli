## PR Title
Write a clear, action-oriented title. This will be used for changelog and release notes.
Examples: "Add `portolan publish` command for S3 sync" or "Fix collection asset path resolution"

## Description
<!-- Concise summary of what changed and why. This may be used in release notes. -->

## Technical Details
<!-- Implementation notes, breaking changes, migration steps. For reviewers. -->

## Breaking Changes
**Does this PR introduce breaking changes?** No / Yes

<!-- If yes, describe what breaks and how users should migrate -->

## Related Issue(s)
- #

## Checklist
See [what a finished PR looks like](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/contributing.md#what-a-finished-pr-looks-like).

- [ ] Tests written **first** and exercise real behavior (TDD)
- [ ] Integration coverage where the change crosses layers
- [ ] `prek run --all-files` is green locally; all required CI checks pass
- [ ] Changed lines are covered (`codecov/patch`)
- [ ] At least one adversarial review (actively tried to break it)
- [ ] CodeRabbit comments addressed
- [ ] Docs updated and, for non-obvious decisions, an ADR added
