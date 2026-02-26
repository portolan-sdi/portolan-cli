# ADR-0024: Hierarchical Config System

## Status
Accepted

## Context

Portolan needs a configuration system for persistent settings like remote storage URLs and AWS credentials profiles. Without config, users must pass `--remote s3://bucket/` on every sync/push/pull command.

### Requirements

1. **Persistent storage**: Settings persist across CLI invocations
2. **Precedence**: CLI args override env vars override config file override defaults
3. **Extensibility**: Any CLI param can become a config key without code changes
4. **Simplicity**: Start with catalog-level config, design for future global/collection tiers

### Forces

- ADR-0023 established `.portolan/` for internal tooling state
- YAML is more human-readable than JSON for config files users may edit
- Validation of settings (e.g., S3 URL format) is better delegated to the tools that use them (aws-cli, boto3)

## Decision

### Location

Config stored in `.portolan/config.yaml` (per ADR-0023's principle of internal tooling state in `.portolan/`).

### Format

YAML, not JSON. Rationale:
- Users may hand-edit config
- YAML supports comments
- More readable for key-value pairs

### Precedence (highest to lowest)

```
CLI argument > Environment variable > Catalog config > Built-in default
```

Environment variable naming: `PORTOLAN_<SETTING>` (e.g., `PORTOLAN_REMOTE`, `PORTOLAN_AWS_PROFILE`)

### Validation Strategy

**Validate on use, not on set.** Rationale:
- `portolan config set remote s3://bucket/` should succeed even if AWS credentials aren't configured
- The sync/push command validates the URL when it actually tries to connect
- Avoids coupling config module to every possible validation rule

### Commands

```bash
portolan config set <key> <value>   # Set in .portolan/config.yaml
portolan config get <key>           # Show resolved value (with precedence)
portolan config list                # Show all settings with sources
portolan config unset <key>         # Remove from config file
```

### Initial Settings

| Key | Description | Env Var |
|-----|-------------|---------|
| `remote` | S3/GCS/Azure URL for sync | `PORTOLAN_REMOTE` |
| `aws_profile` | AWS credentials profile | `PORTOLAN_AWS_PROFILE` |

### API Design

```python
def load_config(catalog_path: Path) -> dict[str, Any]:
    """Load config from .portolan/config.yaml.

    Returns the full config dict including any 'collections' section.
    If file doesn't exist, returns empty dict.
    """

def save_config(catalog_path: Path, config: dict[str, Any]) -> None:
    """Write config to .portolan/config.yaml."""

def get_setting(
    key: str,
    cli_value: Any | None = None,
    catalog_path: Path | None = None,
    collection: str | None = None,
) -> Any | None:
    """Resolve setting with full precedence.

    Precedence (highest to lowest):
    1. CLI argument (cli_value)
    2. Environment variable (PORTOLAN_<KEY>)
    3. Collection config (if collection specified)
    4. Catalog config
    5. Built-in default (None)
    """

def set_setting(
    catalog_path: Path,
    key: str,
    value: Any,
    collection: str | None = None,
) -> None:
    """Set a config value in .portolan/config.yaml.

    If collection is specified, sets in collections.<collection>.<key>.
    """

def unset_setting(
    catalog_path: Path,
    key: str,
    collection: str | None = None,
) -> bool:
    """Remove a config value. Returns True if key existed."""
```

### Error Message (when remote missing)

```
Error: No remote specified.
  Set via: portolan config set remote <url>
  Or pass: --remote <url>
  Or set:  PORTOLAN_REMOTE=<url>
```

This error format:
1. States the problem clearly
2. Provides all three ways to fix it (config, CLI, env)
3. Is parseable for JSON output

## Consequences

### Benefits

- **Consistent CLI ergonomics**: All commands can use `get_setting()` for any parameter
- **Future-proof**: Adding new settings requires no code changes to config module
- **Testable**: Precedence logic is centralized and unit-testable
- **Debuggable**: `portolan config list` shows where each value comes from

### Trade-offs

- **No validation on set**: Users can store invalid values that fail later
- **YAML dependency**: Adds PyYAML to dependencies (though likely already present via other libs)

### Config File Structure

```yaml
# .portolan/config.yaml
remote: s3://bucket/catalog
aws_profile: production

# Collection-level overrides (optional)
collections:
  demographics:
    remote: s3://public-bucket/demographics
  restricted-data:
    aws_profile: high-security
```

Collection-level config lives in a `collections:` section within the same file, not in separate files. This:
- Keeps all config in one place
- Avoids file proliferation
- Makes it easy to see the full config at a glance

### Future Extensions

1. **Global config** (`~/.portolan/config.yaml`): For user-wide defaults
2. **Config profiles**: Named configurations (e.g., `--profile staging`)

## Alternatives Considered

### JSON instead of YAML
**Rejected**: Less readable, no comment support, users may need to hand-edit.

### Validate on set
**Rejected**: Creates coupling between config module and validation logic for each setting. Complicates offline workflows where validation may not be possible.

### INI format
**Rejected**: Less flexible for nested structures if we need them later. YAML is more consistent with modern tooling.
