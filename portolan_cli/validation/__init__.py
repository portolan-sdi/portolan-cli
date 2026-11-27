"""Adapter over rashid, the Portolan conformance validator."""

from __future__ import annotations

from portolan_cli.input_hardening import (
    InputValidationError,
    validate_catalog_id,
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
    remediation_of,
)
from portolan_cli.validation.report import (
    annotate_survivors,
    build_check_payload,
    build_fix_payload,
)
from portolan_cli.validation.runner import CheckOutcome, LiveHint, run_check

__all__ = [
    "DEFAULT_REMEDIATION",
    "FIXERS",
    "RULE_REMEDIATION",
    "Bucket",
    "CheckOutcome",
    "InputValidationError",
    "LiveHint",
    "Remediation",
    "annotate_survivors",
    "apply_fixers",
    "auto_fixer_keys",
    "build_check_payload",
    "build_fix_payload",
    "remediation_for",
    "remediation_of",
    "run_check",
    "validate_catalog_id",
    "validate_collection_id",
    "validate_config_key",
    "validate_config_value",
    "validate_item_id",
    "validate_remote_url",
    "validate_safe_path",
]
