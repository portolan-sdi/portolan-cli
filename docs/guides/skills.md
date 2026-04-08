# AI Skills

Portolan includes **skills** — markdown guides that help AI assistants guide you through complex workflows. Think of them as recipes that Claude, GPT, or other AI agents can follow to help you accomplish tasks.

## Available Skills

| Skill | Description |
|-------|-------------|
| `sourcecoop` | Upload data to [Source Cooperative](https://source.coop) |

## Using Skills

### With Claude Code

If you're using [Claude Code](https://docs.anthropic.com/en/docs/claude-code), simply ask:

> "Help me upload this data to Source Cooperative"

Claude will automatically use the `sourcecoop` skill to guide you through the process.

### Viewing Skills Directly

You can view any skill's content:

```bash
# List available skills
portolan skills list

# View a specific skill
portolan skills show sourcecoop
```

---

## Source Cooperative Skill

The `sourcecoop` skill helps you publish geospatial data to [Source Cooperative](https://source.coop), an open data commons for geospatial data.

### What It Does

1. **Checks credentials** — Verifies you have Source Co-op access configured
2. **Configures remote** — Sets up the S3 destination for your org/product
3. **Creates metadata** — Ensures required fields (title, description, license, contact)
4. **Generates READMEs** — Creates documentation from your metadata
5. **Uploads data** — Pushes to Source Co-op with parallel uploads

### Prerequisites

You need **automated access** to Source Cooperative. If you don't have it yet, contact [hello@source.coop](mailto:hello@source.coop) to request access.

### Quick Example

```bash
# Navigate to your data
cd ~/data/my-dataset

# Initialize catalog
portolan init --title "My Dataset" --auto

# Configure Source Co-op
portolan config set remote "s3://us-west-2.opendata.source.coop/myorg/my-dataset/"
portolan config set profile source-coop

# Add files and create metadata
portolan add .
portolan metadata init --recursive

# Edit .portolan/metadata.yaml with:
#   title, description, license, contact.email

# Generate READMEs and push
portolan readme --recursive
portolan push --workers 8
```

### Required Metadata

Source Co-op emphasizes good documentation. The skill ensures you provide:

| Field | Example |
|-------|---------|
| `title` | Philadelphia 2023 Aerial Orthoimagery |
| `description` | High-resolution aerial imagery covering Philadelphia County |
| `license` | CC-BY-4.0 |
| `contact.email` | data@example.org |

### Troubleshooting

**"Access Denied"** — Check your AWS credentials in `~/.aws/credentials` under `[source-coop]`. Credentials may have expired.

**Slow uploads** — Use `--workers 8` for parallel uploads. More than 8 workers doesn't usually help.

**Missing metadata** — Run `portolan metadata validate` to see which required fields are missing.
