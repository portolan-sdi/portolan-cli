"""Adapter over rashid, the Portolan conformance validator (ADR-0057).

`portolan check` does not implement validation rules. rashid owns the rule set
(``PTL-*`` ids citing ``PORTO-*`` spec requirements) and the structural, schema,
data, and live passes. This package translates between rashid and the CLI:

- :mod:`~portolan_cli.validation.config` — ``.portolan/config.yaml`` to
  ``rashid.RulesConfig``, plus the published base URL.
- :mod:`~portolan_cli.validation.runner` — one call that runs rashid and the
  source-file convertibility check, returning a :class:`CheckOutcome`.
- :mod:`~portolan_cli.validation.remediation` — who fixes which rule id.
- :mod:`~portolan_cli.validation.fixers` — the fixers ``--fix`` dispatches on,
  one per AUTO row in the remediation table.
- :mod:`~portolan_cli.validation.report` — the JSON payload `--json` emits.
- :mod:`~portolan_cli.validation.legacy` — detect pre-schema-URI catalogs.

Nothing here renders output: :mod:`report` returns plain dicts and ``cli.py``
does the printing, so the dependency runs one way (import-linter contract
``validation-is-an-adapter``).

Input hardening (ADR-0030) lives in the top-level
:mod:`portolan_cli.input_hardening` leaf, not here: sanitizing agent-supplied
arguments is not catalog validation. These names are re-exported for the
existing ``from portolan_cli.validation import ...`` call sites:
- InputValidationError: Exception for input validation failures
- validate_safe_path(): Protect against path traversal attacks
- validate_collection_id(): Validate STAC collection IDs
- validate_item_id(): Validate STAC item IDs
- validate_remote_url(): Validate S3/GCS/Azure URLs
- validate_config_key(): Validate config keys
- validate_config_value(): Validate config values
"""

from portolan_cli.input_hardening import (
    InputValidationError,
    validate_collection_id,
    validate_config_key,
    validate_config_value,
    validate_item_id,
    validate_remote_url,
    validate_safe_path,
)
from portolan_cli.validation.fixers import FIXERS, apply_fixers, auto_fixer_keys
from portolan_cli.validation.remediation import (
    DEFAULT_REMEDIATION,
    RULE_REMEDIATION,
    Bucket,
    Remediation,
    remediation_for,
)
from portolan_cli.validation.report import (
    annotate_survivors,
    build_check_payload,
    build_fix_payload,
)
from portolan_cli.validation.runner import CheckOutcome, LiveHint, run_check

__all__ = [
    "Bucket",
    "CheckOutcome",
    "DEFAULT_REMEDIATION",
    "InputValidationError",
    "LiveHint",
    "RULE_REMEDIATION",
    "Remediation",
    "FIXERS",
    "annotate_survivors",
    "apply_fixers",
    "auto_fixer_keys",
    "build_check_payload",
    "build_fix_payload",
    "remediation_for",
    "run_check",
    "validate_collection_id",
    "validate_config_key",
    "validate_config_value",
    "validate_item_id",
    "validate_remote_url",
    "validate_safe_path",
]
