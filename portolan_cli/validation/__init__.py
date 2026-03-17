"""Validation framework for Portolan catalogs.

This module provides the public API for validating catalogs:
- check(): Run validation rules against a catalog
- ValidationReport: Aggregate validation results
- ValidationRule: Base class for custom rules

Per ADR-0011, this is an MVP that validates catalog structure only.
Dataset-specific and remote validation comes in later versions.

Input Hardening:
- InputValidationError: Exception for input validation failures
- validate_safe_path(): Protect against path traversal attacks
- validate_collection_id(): Validate STAC collection IDs
- validate_item_id(): Validate STAC item IDs
- validate_remote_url(): Validate S3/GCS/Azure URLs
- validate_config_key(): Validate config keys
- validate_config_value(): Validate config values
"""

from portolan_cli.validation.input_hardening import (
    InputValidationError,
    validate_collection_id,
    validate_config_key,
    validate_config_value,
    validate_item_id,
    validate_remote_url,
    validate_safe_path,
)
from portolan_cli.validation.results import (
    Severity,
    ValidationReport,
    ValidationResult,
)
from portolan_cli.validation.rules import ValidationRule
from portolan_cli.validation.runner import check

__all__ = [
    "Severity",
    "ValidationReport",
    "ValidationResult",
    "ValidationRule",
    "check",
    "InputValidationError",
    "validate_safe_path",
    "validate_collection_id",
    "validate_item_id",
    "validate_remote_url",
    "validate_config_key",
    "validate_config_value",
]
