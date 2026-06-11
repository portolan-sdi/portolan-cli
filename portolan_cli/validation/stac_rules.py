"""STAC schema validation and best-practices linting rules.

Uses stac-check as the validation engine. Two rules:
- StacSchemaRule: JSON Schema validation (ERROR severity)
- StacLintRule: Best practices checks (configurable severity)
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from stac_check.lint import Linter  # type: ignore[import-untyped]

from portolan_cli.validation.results import Severity, ValidationResult
from portolan_cli.validation.rules import ValidationRule

# Phrase the schema validator emits when it cannot fetch a JSON Schema over the
# network. stac-validator raises ``Could not resolve schema: <uri>. Reason:
# <err>`` (stac_validator/fast_validator.py) for BOTH core spec schemas and
# extension schemas, so the phrase ALONE cannot tell the two apart — the URI
# must be classified (see `_CORE_SCHEMA_HOSTS`). This replaces the earlier
# broad ``failed to resolve`` substring, which matched no real validator output
# and would have tolerated unrelated ``$ref``-resolution document defects.
_SCHEMA_RESOLUTION_ERROR_MARKER = "could not resolve schema"

# Hosts that serve the CORE STAC spec schemas (catalog/collection/item). When
# one of THESE cannot be fetched the document was never validated at all, so we
# must NOT treat it as acceptable — doing so turns `check` into a silent no-op
# whenever schemas.stacspec.org is unreachable (offline CI, outage, or a bogus
# `stac_version`). Only NON-core (extension) schema failures are tolerable: an
# unpublished/proposed extension (STAC Iceberg, the git-backed-catalog
# extension) that 404s leaves the document itself well-formed.
_CORE_SCHEMA_HOSTS: frozenset[str] = frozenset({"schemas.stacspec.org"})

# Pull the failing schema URI out of the marker message so it can be classified
# as a core vs extension schema. The URI runs to the next whitespace; the
# trailing ``.`` before `` Reason:`` is stripped by the caller.
_RESOLUTION_URI_RE = re.compile(
    rf"{_SCHEMA_RESOLUTION_ERROR_MARKER}:\s*(?P<uri>\S+)",
    re.IGNORECASE,
)


def _is_schema_resolution_error(message: str | None) -> bool:
    """True if `message` is a *tolerable* extension-schema fetch failure.

    Tolerable means the schema that could not be fetched is a STAC *extension*
    schema. A failure to fetch a CORE spec schema (``schemas.stacspec.org``)
    means the document was never validated, so it is NOT tolerable — otherwise
    an offline run would pass every catalog, valid or not. If the marker is
    present but the URI cannot be isolated, we are conservative and do not
    tolerate (we can't prove it was only an extension schema).
    """
    if not message:
        return False
    if _SCHEMA_RESOLUTION_ERROR_MARKER not in message.lower():
        return False
    match = _RESOLUTION_URI_RE.search(message)
    if match is None:
        return False
    host = (urlsplit(match.group("uri").rstrip(".")).hostname or "").lower()
    return host not in _CORE_SCHEMA_HOSTS


class StacSchemaRule(ValidationRule):
    """Validate STAC objects against JSON Schema spec.

    Uses stac-check's schema validation (via stac-validator).
    Validates catalog.json and follows STAC link relations to
    validate collections and items.

    Note: Portolan uses relative hrefs by design (for portability). The STAC
    JSON Schema requires absolute IRIs, so IRI format errors are treated as
    acceptable. Use --strict for full IRI validation.
    """

    name = "stac_schema"
    severity = Severity.ERROR
    description = "Validate STAC JSON against official schemas"

    # Errors to treat as acceptable (Portolan uses relative paths by design)
    ACCEPTABLE_ERRORS: frozenset[str] = frozenset(
        {
            "must be iri",  # Relative hrefs are valid in Portolan
            "is not a 'iri'",  # Alternate phrasing
            "list index out of range",  # stac-check bug on Windows with recursive validation
        }
    )

    def __init__(self, *, strict: bool = False) -> None:
        """Initialize rule.

        Args:
            strict: If True, enable full geometry validation AND strict IRI checks.
                    If False, skip geometry checks and accept relative hrefs.
        """
        self.strict = strict

    def _is_acceptable_error(self, error_msg: str) -> bool:
        """Check if error is acceptable (e.g., relative href IRI error)."""
        if self.strict:
            return False  # In strict mode, no errors are acceptable
        error_lower = error_msg.lower()
        return any(pattern in error_lower for pattern in self.ACCEPTABLE_ERRORS)

    def check(self, catalog_path: Path) -> ValidationResult:
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        try:
            linter = Linter(
                item=str(catalog_json),
                recursive=True,
                fast=not self.strict,
            )
        except Exception as e:
            # An unresolvable extension schema (e.g. the STAC Iceberg or
            # proposed git extension) surfaces here as a RuntimeError during
            # construction. Don't fail the document for a schema we can't
            # fetch — unless --strict, where the user opts into full checks.
            if not self.strict and _is_schema_resolution_error(str(e)):
                return self._pass("Schema valid (unresolved extension schema accepted)")
            return self._fail(
                f"STAC validation failed: {e}",
                fix_hint="Check that all STAC files have valid JSON syntax",
            )

        # Check root-level validation
        if not linter.valid_stac:
            error_msg = linter.error_msg or "STAC schema validation failed"
            if self._is_acceptable_error(error_msg):
                return self._pass("Schema valid (relative hrefs accepted)")
            if not self.strict and _is_schema_resolution_error(error_msg):
                return self._pass("Schema valid (unresolved extension schema accepted)")
            recommendation = getattr(linter, "recommendation", None)
            return self._fail(error_msg, fix_hint=recommendation)

        # Check recursive validation results (stac-check stores these separately)
        validate_all = getattr(linter, "validate_all", [])
        failed = [r for r in validate_all if not r.get("valid_stac", True)]

        # Filter out acceptable errors
        real_failures = []
        for f in failed:
            msg = f.get("error_message", "")
            if self._is_acceptable_error(msg):
                continue
            if not self.strict and _is_schema_resolution_error(msg):
                continue
            real_failures.append(f)

        if real_failures:
            first_error = real_failures[0]
            msg = first_error.get("error_message", "Schema validation failed")
            path = first_error.get("path", "unknown")
            hint = first_error.get("recommendation")
            return self._fail(f"{msg} in {path}", fix_hint=hint)

        return self._pass("All STAC objects pass schema validation")


class StacLintRule(ValidationRule):
    """Check STAC objects against best practices.

    Uses stac-check's best practices checks. Each check can have
    configurable severity via .portolan/config.yaml.
    """

    name = "stac_lint"
    severity = Severity.WARNING
    description = "Check STAC against best practices"

    # Checks to skip (handled by other portolan rules)
    SKIP_CHECKS: frozenset[str] = frozenset(
        {
            "datetime_null",  # ProvisionalDatetimeRule handles this
        }
    )

    # Default severity for each check (can be overridden in config)
    DEFAULT_SEVERITIES: dict[str, Severity] = {
        "searchable_identifiers": Severity.ERROR,
        "percent_encoded": Severity.ERROR,
        "check_catalog_id": Severity.WARNING,
        "check_item_id": Severity.WARNING,
        "check_thumbnail": Severity.WARNING,
        "check_links_title": Severity.INFO,
        "check_links_self": Severity.WARNING,
        "null_geometry": Severity.WARNING,
        "check_summaries": Severity.WARNING,
        "bloated_metadata": Severity.INFO,
        "bloated_links": Severity.INFO,
    }

    def __init__(
        self,
        *,
        strict: bool = False,
        config: dict[str, Any] | None = None,
    ) -> None:
        self.strict = strict
        self.config = config or {}
        # Compute skip checks once at init time (not as side effect of _get_severity_map)
        self._runtime_skip_checks: frozenset[str] = self._compute_skip_checks()

    def check(self, catalog_path: Path) -> ValidationResult:
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        try:
            linter = Linter(
                item=str(catalog_json),
                recursive=True,
                fast=not self.strict,
                fast_linting=True,  # Always run BP checks
            )
        except Exception as e:
            # Best-practice linting can't run if an extension schema can't be
            # fetched, but that is not a lint violation — pass rather than
            # block the catalog on an unreachable/unpublished extension.
            if not self.strict and _is_schema_resolution_error(str(e)):
                return self._pass("Lint skipped (unresolved extension schema)")
            return self._fail(f"STAC lint failed: {e}")

        # Get best practices dict (not an attribute, must call method)
        bp_dict = linter.create_best_practices_dict()

        # Collect violations by severity
        errors: list[str] = []
        warnings: list[str] = []
        infos: list[str] = []

        severity_map = self._get_severity_map()
        skip_checks = self.SKIP_CHECKS | self._runtime_skip_checks

        for check_name, messages in bp_dict.items():
            if check_name in skip_checks:
                continue
            if not messages:
                continue

            severity = severity_map.get(check_name, Severity.WARNING)
            message = messages[0] if isinstance(messages, list) else str(messages)

            if severity == Severity.ERROR:
                errors.append(f"{check_name}: {message}")
            elif severity == Severity.WARNING:
                warnings.append(f"{check_name}: {message}")
            else:
                infos.append(f"{check_name}: {message}")

        if not errors and not warnings:
            return self._pass("All best practice checks passed")

        # Build summary message
        parts = []
        if errors:
            parts.append(f"{len(errors)} error(s)")
        if warnings:
            parts.append(f"{len(warnings)} warning(s)")

        all_issues = errors + warnings
        detail = "; ".join(all_issues[:3])
        if len(all_issues) > 3:
            detail += f" (+{len(all_issues) - 3} more)"

        return ValidationResult(
            rule_name=self.name,
            passed=len(errors) == 0,
            severity=Severity.ERROR if errors else Severity.WARNING,
            message=f"Best practice issues: {', '.join(parts)}. {detail}",
            fix_hint="Run with --verbose to see all issues",
        )

    def _compute_skip_checks(self) -> frozenset[str]:
        """Compute checks to skip based on config (called once at init)."""
        skip = set()
        overrides = self.config.get("stac_lint", {}).get("severity", {})
        for check_name, level in overrides.items():
            if isinstance(level, str) and level.lower() == "skip":
                skip.add(check_name)
        return frozenset(skip)

    def _get_severity_map(self) -> dict[str, Severity]:
        """Get severity map, merging defaults with config overrides."""
        result = dict(self.DEFAULT_SEVERITIES)

        overrides = self.config.get("stac_lint", {}).get("severity", {})
        for check_name, level in overrides.items():
            if isinstance(level, str):
                level_lower = level.lower()
                if level_lower != "skip":
                    try:
                        result[check_name] = Severity(level_lower)
                    except ValueError:
                        pass  # Invalid severity, keep default

        return result


# Namespace-prefixed technical names like "ns:LayerName" (a GeoServer/WFS habit).
_NAMESPACE_PREFIX_RE = re.compile(r"^[a-z0-9_]+:[A-Za-z]")


def _is_raw_slug_title(title: str) -> bool:
    """True if a title is an un-humanized raw slug (Issue #502).

    Intentionally narrow: a title is only "raw" when it still carries the
    markers that :func:`portolan_cli.humanize.humanize_slug` strips —
    underscores (``publico_arbolado``) or a namespace prefix (``ns:Layer``).
    This keeps the rule consistent with the auto-fix: a humanized title never
    contains those markers, so ``check --fix`` always converges (no looping on
    short ids like ``T502`` that cannot be humanized further).
    """
    stripped = title.strip()
    return "_" in stripped or bool(_NAMESPACE_PREFIX_RE.match(stripped))


class MandatoryTitlesRule(ValidationRule):
    """Require human-readable titles + descriptions across the catalog (Issue #502).

    STAC Browser renders ``child``/``item`` link titles directly; without them
    it must fetch every child just to show its name. Portolan therefore makes
    titles mandatory and human-readable. This rule fails (ERROR) when:

    - a catalog or collection is missing a title, has a technical-looking title
      (e.g. a raw ``snake_case`` slug), or is missing a description, or
    - any ``child``/``item`` link is missing a ``title``.

    ``portolan check --fix`` repairs all of these (see
    :func:`portolan_cli.metadata.fix.repair_titles_and_links`).
    """

    name = "mandatory_titles"
    severity = Severity.ERROR
    description = "Verify catalogs/collections and child/item links have human-readable titles"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Walk the catalog and flag missing/raw-slug titles + untitled links."""
        problems: list[str] = []

        stac_files = sorted(catalog_path.rglob("catalog.json")) + sorted(
            catalog_path.rglob("collection.json")
        )
        for stac_file in stac_files:
            try:
                data = json.loads(stac_file.read_text())
            except (OSError, json.JSONDecodeError):
                # Malformed JSON is reported by other rules; skip here.
                continue
            rel = stac_file.relative_to(catalog_path)
            problems.extend(self._check_object(data, rel))
            problems.extend(self._check_links(data, rel))

        if problems:
            preview = "; ".join(problems[:5])
            if len(problems) > 5:
                preview += f" (+{len(problems) - 5} more)"
            return self._fail(
                f"{len(problems)} title/description issue(s): {preview}",
                fix_hint="Run 'portolan check --fix' to populate human-readable titles",
            )
        return self._pass("All catalogs, collections, and links have human-readable titles")

    @staticmethod
    def _check_object(data: dict[str, Any], rel: Path) -> list[str]:
        """Check a catalog/collection object's own title + description."""
        problems: list[str] = []
        title = data.get("title")
        if not isinstance(title, str) or not title.strip():
            problems.append(f"{rel}: missing title")
        elif _is_raw_slug_title(title):
            problems.append(f"{rel}: title is a raw slug, not human-readable ('{title}')")
        description = data.get("description")
        if not isinstance(description, str) or not description.strip():
            problems.append(f"{rel}: missing description")
        return problems

    @staticmethod
    def _check_links(data: dict[str, Any], rel: Path) -> list[str]:
        """Check that every child/item link carries a title."""
        problems: list[str] = []
        links = data.get("links", [])
        if not isinstance(links, list):
            return problems
        for link in links:
            if not isinstance(link, dict) or link.get("rel") not in ("child", "item"):
                continue
            link_title = link.get("title")
            if not isinstance(link_title, str) or not link_title.strip():
                href = link.get("href", "?")
                problems.append(f"{rel}: {link.get('rel')} link '{href}' missing title")
        return problems
