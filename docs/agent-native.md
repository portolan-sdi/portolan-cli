# Agent-Native CLI Design

Portolan is designed to be **agent-friendly** from the ground up. AI agents are first-class users of the CLI, with structured output, input validation, and self-describing interfaces.

## Key Principles

1. **Structured Output** — All commands support `--json` for machine-readable output
2. **Input Hardening** — Protection against agent hallucinations (path traversals, control characters, etc.)
3. **Predictable Behavior** — Same input always produces same output
4. **Self-Describing** — Commands document themselves through `--help`

## JSON Output

Every command supports JSON output via two mechanisms:

### Global Format Flag

```bash
portolan --format=json <command>
```

### Per-Command Flag

```bash
portolan <command> --json
```

### Output Structure

All JSON output follows a consistent envelope structure:

```json
{
  "success": true|false,
  "command": "command_name",
  "data": {
    // Command-specific payload
  },
  "errors": [  // Only present when success=false
    {
      "type": "ErrorClassName",
      "message": "Human-readable description",
      "code": "PRTLN-ERR001"  // Optional structured code
    }
  ]
}
```

### Example: Successful Scan

```bash
$ portolan scan . --json
{
  "success": true,
  "command": "scan",
  "data": {
    "files": 15,
    "ready": 12,
    "issues": 3,
    "collections": ["demographics", "imagery"]
  }
}
```

### Example: Error

```bash
$ portolan check /nonexistent --json
{
  "success": false,
  "command": "check",
  "data": {},
  "errors": [
    {
      "type": "PathNotFoundError",
      "message": "Path does not exist: /nonexistent"
    }
  ]
}
```

## Input Validation

Portolan validates all inputs to protect against common agent hallucination patterns identified in [agent-native CLI research](https://jpoehnelt.dev/blog/agent-native-cli/).

### Protected Against

| Threat | Example | Protection |
|--------|---------|------------|
| **Path traversals** | `../../.ssh/id_rsa` | `validate_safe_path()` |
| **Control characters** | `census\x00data` | Reject ASCII < 0x20 |
| **Query params in IDs** | `census?fields=name` | Reject `?` and `#` |
| **Pre-encoded strings** | `%2e%2e` (double-encoded `..`) | Reject `%` in IDs |
| **Path separators in IDs** | `parent/child` | Collections are flat |

### Validation Functions

All validation functions are available from `portolan_cli.validation`:

```python
from portolan_cli.validation import (
    InputValidationError,
    validate_safe_path,
    validate_collection_id,
    validate_item_id,
    validate_remote_url,
    validate_config_key,
    validate_config_value,
)

# Example: Validate collection ID before use
try:
    collection_id = validate_collection_id(user_input)
except InputValidationError as e:
    print(f"Invalid input: {e}")
```

## Exit Codes

Portolan uses consistent exit codes regardless of output format:

- **0** — Success
- **1** — Error (user error, file not found, validation failure, etc.)

**Never rely on exit codes for semantic information.** Parse the JSON `success` field and `errors` array instead.

## Dry-Run Mode

All mutating commands support `--dry-run` to preview operations without executing them:

```bash
portolan scan . --dry-run      # Preview validation issues
portolan add data/ --dry-run   # Preview tracking changes (planned)
portolan push --dry-run        # Preview sync operations
portolan clean --dry-run       # Preview metadata removal
```

**Use case for agents:** "Think out loud" before mutating operations to validate the plan.

## Self-Describing Interface

Every command provides comprehensive help:

```bash
portolan --help              # List all commands
portolan scan --help         # Command-specific help
portolan config set --help   # Subcommand help
```

Help output includes:

- Command description
- All available flags and options
- Usage examples
- Output format information

## Agent Workflow Examples

### Scan → Check → Add → Push

```bash
# 1. Scan directory for issues
portolan scan /data/census --json > scan_result.json

# 2. Parse results (agent reads JSON)
issues=$(jq '.data.issues' scan_result.json)

# 3. If clean, check STAC validity
if [ "$issues" -eq "0" ]; then
  portolan check /data/census --json > check_result.json
fi

# 4. Track files
portolan add /data/census --json > add_result.json

# 5. Push to cloud
portolan push s3://bucket/catalog --collection census --json
```

### Error Handling

```python
import json
import subprocess

result = subprocess.run(
    ["portolan", "scan", "/data", "--json"],
    capture_output=True,
    text=True
)

data = json.loads(result.stdout)

if not data["success"]:
    # Handle errors
    for error in data["errors"]:
        print(f"{error['type']}: {error['message']}")
    sys.exit(1)

# Process successful result
files = data["data"]["files"]
print(f"Scanned {files} files")
```

## Design References

Portolan's agent-native design follows best practices from:

- **CLI-Anything** ([research](https://arxiv.org/html/2603.05344v1)) — Structured JSON output, deterministic behavior
- **Google Workspace CLI** ([blog post](https://jpoehnelt.dev/blog/agent-native-cli/)) — Input hardening against hallucinations
- **MCP Protocol** ([Speakeasy guide](https://www.speakeasy.com/mcp/using-mcp/ai-agents/architecture-patterns)) — Self-describing interfaces

## Comparison: Human vs. Agent Interface

| Feature | Human-First | Agent-First | Portolan |
|---------|-------------|-------------|----------|
| **Output** | Colored text, tables | JSON envelopes | Both (default text, `--json` for agents) |
| **Errors** | stderr | structured errors | Both (stderr for text, JSON envelope for agents) |
| **Validation** | Forgiving | Strict | Strict always (protects both) |
| **Help** | Human-readable | Machine-parseable | Both (plain text help) |
| **Flags** | Convenience shortcuts | Raw payloads | Both (flags + JSON payloads where applicable) |

## Future Enhancements

Potential future agent-native features (not yet implemented):

1. **Schema introspection** — `portolan schema <command>` returns JSON Schema
2. **Field masks** — `portolan list --fields id,name` limits output size
3. **MCP surface** — Expose CLI as Model Context Protocol tools
4. **Response sanitization** — Protect against prompt injection in API responses

See the [roadmap](https://github.com/portolan-sdi/portolan-cli/issues?q=label%3Aroadmap) for planned features.
