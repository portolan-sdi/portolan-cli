## Install

### Claude Code (CLI & Desktop)

```bash
claude plugin marketplace add portolan-sdi/portolan-skills
claude plugin install portolan
```

Skills become available as `portolan:portolan-cli`, `portolan:reading-portolan`, `portolan:portolan-bootstrap`, `portolan:portolan-consume`, and `portolan:sourcecoop`.

### Claude Code (Web / Cowork)

The web app at [claude.ai/code](https://claude.ai/code) does not currently support plugin installation. To use these skills in Cowork, paste the content of a SKILL.md file into your project's `CLAUDE.md` or provide it as context.

### Gemini CLI

Gemini CLI natively supports the same `SKILL.md` format:

```bash
# Install skills at user scope
gemini skills install https://github.com/portolan-sdi/portolan-skills.git \
  --path skills/portolan-cli --consent
gemini skills install https://github.com/portolan-sdi/portolan-skills.git \
  --path skills/reading-portolan --consent
gemini skills install https://github.com/portolan-sdi/portolan-skills.git \
  --path skills/portolan-bootstrap --consent
gemini skills install https://github.com/portolan-sdi/portolan-skills.git \
  --path skills/portolan-consume --consent
gemini skills install https://github.com/portolan-sdi/portolan-skills.git \
  --path skills/sourcecoop --consent

# Or at workspace scope (shared via version control)
gemini skills install https://github.com/portolan-sdi/portolan-skills.git \
  --path skills/reading-portolan --scope workspace --consent
```

### OpenAI Codex CLI

Codex CLI also supports `SKILL.md` files. Copy the skills into your project's `.agents/skills/` directory:

```bash
# Clone and copy into your project
git clone https://github.com/portolan-sdi/portolan-skills.git /tmp/portolan-skills
mkdir -p .agents/skills
cp -r /tmp/portolan-skills/skills/* .agents/skills/
```

### Any AI Agent (Manual)

The skills are just markdown files. For any AI coding tool that supports custom instructions or system prompts:

1. Copy the content of the relevant `SKILL.md` file
2. Add it to your tool's custom instructions, system prompt, or project context file (`CLAUDE.md`, `GEMINI.md`, `AGENTS.md`, `.cursorrules`, etc.)
