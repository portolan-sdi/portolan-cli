<!-- ops-sync:begin — synced from portolan-sdi/portolan-ops. Edit there, not here. -->
# Portolan agent norms

Canonical rules for AI agents working in any portolan-sdi repo. Downstream repos carry this text verbatim as a synced block at the top of their own `AGENTS.md`, so the rules are in context rather than a link away. Repo-specific instructions live below that block. When a repo-specific rule conflicts with this file, the repo-specific rule wins for that repo.

Claude Code does not read `AGENTS.md`. Each repo therefore carries a one-line `CLAUDE.md` that imports it. Put repo-specific instructions in `AGENTS.md`, never in `CLAUDE.md`, which the sync overwrites.

## Voice and prose

- All collective public-facing copy (website, announcements, docs, presentations) follows [VOICE.md](https://github.com/portolan-sdi/portolan-ops/blob/main/VOICE.md). Read it before writing any of those.
- How Portolan is described comes from [copy/messaging.md](https://github.com/portolan-sdi/portolan-ops/blob/main/copy/messaging.md) alone. That file is provisional but authoritative: it distills the working messaging document and wins over any older copy anywhere in the org. Never describe Portolan from memory or from copy that predates it.
- All written artifacts (READMEs, PR and issue bodies, docs, commit message bodies, lasting code comments) follow [STYLE.md](https://github.com/portolan-sdi/portolan-ops/blob/main/STYLE.md). Apply it while drafting, not as a cleanup pass.
- Both are mandatory. "Agents MUST abide" is the operative phrase in each.

## Writing issues and pull requests

A reviewer should finish a pull request body in under a minute and know what changed, why, and that it works. Two rules make that possible, and CI checks both on every push and edit.

- **200 words outside code blocks, no section longer than six lines.** Fenced blocks are uncapped, so evidence never competes with the budget. Say the thing once. Do not restate the diff, do not summarize your own summary, and do not explain the approach at a level the code already shows.
- **Show that it works on real data.** Paste the command and the output you got, and name the data it read: a URL or a catalog path. Green tests are not verification. A change that alters no behavior waives this by ticking the waiver checkbox in the template.

Issues carry the same budget. A bug report needs the reproduction that triggered it, a feature request needs the transcript showing where current behavior falls short, and a task needs the command that will prove it done. Every repo runs these forms, and blank issues are off.

The check fails the pull request. On an issue it applies `needs-rewrite` and comments once.

## Documentation

Agents writing or restructuring documentation, READMEs above all, MUST follow the two guidance sources named in [norms/docs.md](https://github.com/portolan-sdi/portolan-ops/blob/main/norms/docs.md):

1. **[obstore](https://github.com/developmentseed/obstore)** is the exemplar. Before drafting, fetch and study its README and docs layout. Match its shape: what belongs on a landing page, how quick-start is separated from deep documentation and API reference, how much each layer says.
2. **[scaffold-docs-skill](https://github.com/dbreunig/scaffold-docs-skill)** is the method. Draft top-down in layers: section structure first, then headers, then topic sentences, then paragraphs, pausing for human review between layers rather than emitting finished pages in one pass.

Do not draft a README from a generic template or from memory of "what READMEs look like." Consult both sources first, every time.

## Org-wide facts

- License is Apache-2.0 in every repo. Never introduce code under another license without a human decision recorded in `norms/repos.md`.
- The canonical homepage is https://www.portolan-sdi.org/. Canonical URLs live in [copy/urls.md](https://github.com/portolan-sdi/portolan-ops/blob/main/copy/urls.md). Do not hardcode variants.
- Community discussion happens in the [Portolan Google Group](https://groups.google.com/g/portolan) and the [Portolan channel](https://cloudnativegeo.slack.com/archives/C0A1JBH9529) in the Cloud-Native Geo Slack. Planning lives in [org-level GitHub projects](https://github.com/orgs/portolan-sdi/projects/1).
- The [portolan-spec](https://github.com/portolan-sdi/portolan-spec) repo is the ground truth for the Portolan standard. The CLI, the validator, the registry, and every other tool implement the spec and are downstream of it. Never describe the CLI as the source of truth for the spec. Propose spec changes in portolan-spec.

## Contribution rules

- The [AI policy](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/AI_POLICY.md) applies to every contribution. A human must have read, reviewed, and understood any change before review is requested. Agents never open PRs, post comments, or take action in shared spaces without human approval.
- Follow the [contributing guide](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/CONTRIBUTING.md) and the [code of conduct](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/CODE_OF_CONDUCT.md).
- Conventional commits. Squash-merge means the PR title is the commit message. Write it in conventional form.
- Never bypass pre-commit hooks or CI gates. Green means green.

## Ground truth discipline

- One canonical home per fact. Link, don't duplicate. If a value (a color, a URL, a policy line) exists in this repo, reference it rather than copying it.
- Shared files reach downstream repos through `sync/manifest.yml` and the sync workflow, never by hand-copying. To change a synced file in a downstream repo, change it here.
- Brand values come from `brand/brand.json`. Regenerate derived files (`brand/emit_css.py`) rather than editing them.
<!-- ops-sync:end -->
