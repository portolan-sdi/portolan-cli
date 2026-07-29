# Agent threat model

AI agents are primary users of the Portolan CLI. They fail differently from
humans, and `portolan_cli/input_hardening.py` is stricter than a human-only CLI
would need. This note records why, because the validators themselves show what
is rejected without showing what they defend against.

The operating assumption: agents are not trusted operators. Inputs are
adversarial until validated.

## Observed failure patterns

Agents produce four input defects that humans rarely produce:

- **Path traversal.** An agent confuses context and emits `../../.ssh`.
- **Embedded query parameters.** A resource identifier arrives as
  `census?fields=name`, carrying URL syntax into a slug.
- **Double encoding.** An agent pre-encodes a string that then gets encoded
  again, producing `%2e%2e`.
- **Control characters.** Invisible characters reach output and corrupt it
  downstream.

Each of these turns into a destructive filesystem or remote operation if it
reaches business logic unchecked.

## The validators

Six functions in `portolan_cli/input_hardening.py` run at CLI entry points,
before any business logic:

| Function | Guards against |
|----------|----------------|
| `validate_safe_path` | Traversal; canonicalizes against a base directory |
| `validate_collection_id` | Control characters, query parameters, encoding tricks |
| `validate_item_id` | Same class, slightly more permissive |
| `validate_remote_url` | Malformed S3, GCS, and Azure URLs; traversal in keys |
| `validate_config_key` | Anything outside lowercase alphanumerics and underscores |
| `validate_config_value` | Context-specific checks, such as URL shape for `remote` |

They raise `InputValidationError`, a `ValueError` subclass. The JSON error
envelope reports the error class name, so the exception type is part of the
agent contract.

## Why the strictness is deliberate

The validators reject inputs that are theoretically valid, uppercase collection
identifiers among them. That is the intended trade. Agents benefit more from
rules they cannot bend than from permissive parsing that occasionally accepts a
hallucination.

Encoding a rule as a validator also outlasts documenting it. `validate_collection_id`
enforces itself; a sentence telling agents to validate collection identifiers
does not.

## Related

The JSON envelope that carries these errors is described in
`.claude/rules/cli-and-output.md`. Every user-supplied path argument must reach
`validate_safe_path` before use.
