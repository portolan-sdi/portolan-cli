## Installation

Choose the instructions for your agent. Copy whole skill directories so the agent can use their scripts and reference files.

### Claude CLI

For Claude Code, use the plugin commands in the [quickstart](#quickstart), then start a new session. Skills use the `portolan:` prefix: `/portolan:portolan-bootstrap` builds a catalog, and `/portolan:reading-portolan` helps you use one.

See [Claude Code plugin installation](https://code.claude.com/docs/en/discover-plugins) for installation scopes and updates.

### Claude web and desktop apps

1. Open **Customize**, then **Plugins**.
2. Select **Add**, **Add marketplace**, then **Add from a repository**.
3. Enter `portolan-sdi/portolan-skills` and install the **Portolan** plugin.

See [Use plugins in Claude](https://support.claude.com/en/articles/13837440-use-plugins-in-claude) for account and workspace requirements. For workflows that run local tools or process files, use Cowork or Claude Code with access to your working directory.

### Codex

From the root of the project where you want to use the skills, run:

```bash
skills_checkout=$(mktemp -d)
git clone https://github.com/portolan-sdi/portolan-skills.git "$skills_checkout"
mkdir -p .agents/skills
cp -R "$skills_checkout"/skills/. .agents/skills/
```

Codex discovers skills in `.agents/skills/`. Invoke one by name, for example `$portolan-bootstrap`, or describe your task so Codex can select it. For installation across your projects, use `~/.agents/skills/` as the destination. See the [official skill documentation](https://learn.chatgpt.com/docs/build-skills) for discovery and invocation details.

### Gemini CLI

Install a skill at user scope:

```bash
gemini skills install https://github.com/portolan-sdi/portolan-skills.git \
  --path skills/portolan-bootstrap --consent
```

Repeat the command with a directory from the [skills list](#skills) for each skill you need. For the quickstart, also install `git-backed-catalog`, `portolan-cli`, `portolan-thumbnails`, and `sourcecoop`. Add `--scope workspace` to install for the current project. See [Gemini CLI skills](https://geminicli.com/docs/cli/skills/) for discovery and management.

### Other agents

If your agent supports Agent Skills, install the skill directories in its documented location. Otherwise, add the relevant `SKILL.md` content to its custom instructions and make the supporting scripts and references available in the workspace.

<!-- BEGIN GENERATED: skills -->
