"""Metadata YAML schema and validation (ADR-0038).

This module provides validation and template generation for .portolan/metadata.yaml
files. These files contain ONLY human-enrichable fields that can't be derived from
STAC or other sources.

**Required fields (human-only):**
- contact.name, contact.email - Accountability
- license - SPDX identifier

**Auto-filled from STAC (NOT in metadata.yaml):**
- title, description - From catalog/collection init
- columns - From table:columns extension
- bands - From eo:bands, raster:bands extensions
- bbox, CRS, temporal extent - From STAC extent

**Optional enrichment (human):**
- license_url, citation, doi, keywords, attribution
- source_url, processing_notes, known_issues

Usage:
    from portolan_cli.metadata_yaml import validate_metadata, load_and_validate_metadata

    # Validate a metadata dict
    errors = validate_metadata(metadata_dict)

    # Load from hierarchy and validate
    metadata, errors = load_and_validate_metadata(collection_path, catalog_root)
"""

from __future__ import annotations

import logging
import math
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from portolan_cli.config import load_merged_metadata

logger = logging.getLogger(__name__)

# =============================================================================
# Required fields per ADR-0038 (revised)
# Title and description come from STAC, not metadata.yaml
# =============================================================================

REQUIRED_FIELDS = frozenset({"contact", "license"})
REQUIRED_CONTACT_FIELDS = frozenset({"name", "email"})

# =============================================================================
# SPDX License identifiers (common subset)
# Full list: https://spdx.org/licenses/
# =============================================================================

COMMON_SPDX_LICENSES = frozenset(
    {
        # Creative Commons
        "CC0-1.0",
        "CC-BY-4.0",
        "CC-BY-SA-4.0",
        "CC-BY-NC-4.0",
        "CC-BY-NC-SA-4.0",
        "CC-BY-ND-4.0",
        "CC-BY-NC-ND-4.0",
        # Open source
        "MIT",
        "Apache-2.0",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "GPL-2.0-only",
        "GPL-2.0-or-later",
        "GPL-3.0-only",
        "GPL-3.0-or-later",
        "LGPL-2.1-only",
        "LGPL-2.1-or-later",
        "LGPL-3.0-only",
        "LGPL-3.0-or-later",
        "MPL-2.0",
        "ISC",
        "Unlicense",
        # Public domain / open data
        "PDDL-1.0",
        "ODbL-1.0",
        "ODC-By-1.0",
        # Government
        "CC-PDDC",
    }
)

# =============================================================================
# Validation regex patterns
# =============================================================================

# Basic email pattern - not RFC 5322 compliant but catches obvious errors
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# DOI pattern: 10.XXXX/suffix (where XXXX is 4+ digits)
# See: https://www.doi.org/doi_handbook/2_Numbering.html
DOI_PATTERN = re.compile(r"^10\.\d{4,}/\S+$")

# LicenseRef pattern: LicenseRef-[idstring] per SPDX spec Section 6
# idstring: alphanumeric plus dot, hyphen; must have at least one character
# See: https://spdx.github.io/spdx-spec/v2.3/other-licensing-information-detected/
LICENSEREF_PATTERN = re.compile(r"^LicenseRef-[A-Za-z0-9.\-]+$")

# ISO date pattern: YYYY-MM-DD
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# ORCID pattern: 0000-0000-0000-0000 (four groups of 4 digits)
# See: https://support.orcid.org/hc/en-us/articles/360006897674
ORCID_PATTERN = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{4}$")


# =============================================================================
# Validation
# =============================================================================


def _validate_authors(authors: Any) -> list[str]:
    """Validate the 'authors' field (list of author dicts).

    Each author must have 'name'. Optional 'orcid' and 'email' are validated
    for format if present.

    Args:
        authors: The authors field value to validate.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    if not isinstance(authors, list):
        errors.append("Field 'authors' must be a list")
        return errors

    for i, author in enumerate(authors):
        if not isinstance(author, dict):
            errors.append(f"Author entry {i} must be a mapping with 'name' field")
            continue

        # Name is required
        if "name" not in author:
            errors.append(f"Author entry {i} is missing required 'name' field")
        elif not author["name"] or not str(author["name"]).strip():
            errors.append(f"Author entry {i} has empty 'name' field")

        # Validate ORCID format if present
        orcid = author.get("orcid")
        if orcid and str(orcid).strip():
            if not ORCID_PATTERN.match(str(orcid)):
                errors.append(
                    f"Author entry {i} has invalid ORCID format: '{orcid}'. "
                    f"ORCIDs should be like '0000-0001-2345-6789'"
                )

        # Validate email format if present
        email = author.get("email")
        if email and str(email).strip():
            if not EMAIL_PATTERN.match(str(email)):
                errors.append(f"Author entry {i} has invalid email format: '{email}'")

    return errors


def _validate_related_dois(related_dois: Any) -> list[str]:
    """Validate the 'related_dois' field (list of DOI strings).

    Args:
        related_dois: The related_dois field value to validate.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    if not isinstance(related_dois, list):
        errors.append("Field 'related_dois' must be a list")
        return errors

    for i, doi in enumerate(related_dois):
        if not isinstance(doi, str):
            errors.append(f"Item {i} in 'related_dois' must be a string")
            continue
        if not DOI_PATTERN.match(doi):
            errors.append(
                f"Invalid DOI format in 'related_dois[{i}]': '{doi}'. "
                f"DOIs should be like '10.5281/zenodo.1234567'"
            )

    return errors


def _validate_citations(citations: Any) -> list[str]:
    """Validate the 'citations' field (list of citation strings).

    Args:
        citations: The citations field value to validate.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    if not isinstance(citations, list):
        errors.append("Field 'citations' must be a list of strings")
        return errors

    for i, citation in enumerate(citations):
        if not isinstance(citation, str):
            errors.append(f"Citation entry {i} must be a string")

    return errors


def _validate_upstream_version(upstream_version: Any) -> list[str]:
    """Validate the 'upstream_version' field (string).

    Args:
        upstream_version: The upstream_version field value to validate.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    if not isinstance(upstream_version, str):
        errors.append(
            f"Field 'upstream_version' must be a string, got {type(upstream_version).__name__}"
        )

    return errors


def _validate_contact(metadata: dict[str, Any]) -> list[str]:
    """Validate the required 'contact' field.

    Contact must be a dict with 'name' and 'email' subfields.
    Email format is validated.

    Args:
        metadata: The full metadata dictionary.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    if "contact" not in metadata:
        errors.append("Required field 'contact' is missing")
        return errors

    contact = metadata.get("contact")
    if not isinstance(contact, dict):
        errors.append("Field 'contact' must be a mapping with 'name' and 'email'")
        return errors

    for subfield in REQUIRED_CONTACT_FIELDS:
        if subfield not in contact:
            errors.append(f"Required field 'contact.{subfield}' is missing")
        elif not contact[subfield] or not str(contact[subfield]).strip():
            errors.append(f"Field 'contact.{subfield}' cannot be empty")

    # Validate email format if present
    email = contact.get("email")
    if email and not EMAIL_PATTERN.match(str(email)):
        errors.append(f"Invalid email format: '{email}'")

    return errors


def _validate_license(metadata: dict[str, Any]) -> list[str]:
    """Validate the required 'license' field.

    License must be a valid SPDX identifier or LicenseRef-* custom identifier.

    Args:
        metadata: The full metadata dictionary.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    if "license" not in metadata:
        errors.append("Required field 'license' is missing")
        return errors

    if not metadata["license"] or not str(metadata["license"]).strip():
        errors.append("Field 'license' cannot be empty")
        return errors

    # Validate license is SPDX identifier or valid LicenseRef-* custom identifier
    license_id = str(metadata.get("license"))
    is_standard_license = license_id in COMMON_SPDX_LICENSES
    is_custom_license = LICENSEREF_PATTERN.match(license_id) is not None
    if not is_standard_license and not is_custom_license:
        errors.append(
            f"Invalid SPDX license identifier: '{license_id}'. "
            f"Use a standard license (MIT, Apache-2.0, CC-BY-4.0, CC0-1.0) "
            f"or custom format LicenseRef-YourLicense"
        )

    return errors


def _validate_doi(metadata: dict[str, Any]) -> list[str]:
    """Validate the optional 'doi' field format.

    Args:
        metadata: The full metadata dictionary.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    doi = metadata.get("doi")
    if doi and str(doi).strip():
        if not DOI_PATTERN.match(str(doi)):
            errors.append(
                f"Invalid DOI format: '{doi}'. DOIs should be like '10.5281/zenodo.1234567'"
            )

    return errors


def validate_metadata(metadata: dict[str, Any]) -> list[str]:
    """Validate a metadata dictionary against the schema.

    Checks for:
    - Required fields: contact (name + email), license
    - Format validation: email, SPDX license, DOI (if present)

    Note: title and description are NOT required in metadata.yaml - they
    come from STAC catalog/collection metadata.

    Args:
        metadata: The metadata dictionary to validate.

    Returns:
        List of validation error messages. Empty list if valid.
    """
    errors: list[str] = []

    # Required fields
    errors.extend(_validate_contact(metadata))
    errors.extend(_validate_license(metadata))

    # Optional fields with format validation
    errors.extend(_validate_doi(metadata))

    # Validate defaults section if present (optional)
    defaults = metadata.get("defaults")
    if defaults is not None:
        if not isinstance(defaults, dict):
            errors.append("Field 'defaults' must be a mapping")
        else:
            errors.extend(_validate_defaults(defaults))

    # Validate #316 fields if present (all optional)
    if (authors := metadata.get("authors")) is not None:
        errors.extend(_validate_authors(authors))

    if (related_dois := metadata.get("related_dois")) is not None:
        errors.extend(_validate_related_dois(related_dois))

    if (citations := metadata.get("citations")) is not None:
        errors.extend(_validate_citations(citations))

    if (upstream_version := metadata.get("upstream_version")) is not None:
        errors.extend(_validate_upstream_version(upstream_version))

    return errors


def _validate_temporal_defaults(temporal: dict[str, Any]) -> list[str]:
    """Validate the 'defaults.temporal' section.

    Args:
        temporal: The temporal defaults dictionary.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    # Validate year (must be integer in reasonable range)
    year = temporal.get("year")
    if year is not None:
        if not isinstance(year, int):
            errors.append(
                f"Field 'defaults.temporal.year' must be an integer, got {type(year).__name__}"
            )
        elif year < 1800 or year > 2100:
            errors.append(
                f"Field 'defaults.temporal.year' must be between 1800 and 2100, got {year}"
            )

    # Validate start/end dates (must be valid ISO dates)
    for field in ("start", "end"):
        date_val = temporal.get(field)
        if date_val is None:
            continue
        if not isinstance(date_val, str):
            errors.append(f"Field 'defaults.temporal.{field}' must be a date string (YYYY-MM-DD)")
        elif not ISO_DATE_PATTERN.match(date_val):
            errors.append(
                f"Invalid date format for 'defaults.temporal.{field}': '{date_val}'. "
                f"Use ISO format YYYY-MM-DD"
            )
        else:
            # Regex matched, but verify it's an actual valid date
            try:
                date.fromisoformat(date_val)
            except ValueError:
                errors.append(
                    f"Invalid date for 'defaults.temporal.{field}': '{date_val}'. "
                    f"Date does not exist (e.g., month 13 or day 32)"
                )

    # Warn if both year and start are specified (year takes precedence)
    if temporal.get("year") is not None and temporal.get("start") is not None:
        errors.append(
            "Both 'defaults.temporal.year' and 'defaults.temporal.start' specified. "
            "'year' takes precedence - remove 'start' to avoid confusion"
        )

    return errors


def _validate_nodata_value(val: Any, index: int | None = None) -> str | None:
    """Validate a single nodata value.

    Args:
        val: The nodata value to validate.
        index: If provided, the band index (for per-band nodata).

    Returns:
        Error message if invalid, None if valid.
    """
    field = f"defaults.raster.nodata[{index}]" if index is not None else "defaults.raster.nodata"

    if val is None:
        return None
    if not isinstance(val, (int, float)):
        return f"Field '{field}' must be a number, got {type(val).__name__}"
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return f"Field '{field}' must be a finite number, not NaN or Infinity"
    return None


def _validate_raster_defaults(raster: dict[str, Any]) -> list[str]:
    """Validate the 'defaults.raster' section.

    Args:
        raster: The raster defaults dictionary.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    nodata = raster.get("nodata")
    if nodata is None:
        return errors

    if isinstance(nodata, list):
        if len(nodata) == 0:
            errors.append("Field 'defaults.raster.nodata' list cannot be empty")
        else:
            for i, val in enumerate(nodata):
                if err := _validate_nodata_value(val, index=i):
                    errors.append(err)
    elif err := _validate_nodata_value(nodata):
        errors.append(err)

    return errors


def _validate_defaults(defaults: dict[str, Any]) -> list[str]:
    """Validate the 'defaults' section of metadata.yaml.

    Args:
        defaults: The defaults dictionary to validate.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    # Validate temporal defaults
    temporal = defaults.get("temporal")
    if temporal is not None:
        if not isinstance(temporal, dict):
            errors.append("Field 'defaults.temporal' must be a mapping")
        else:
            errors.extend(_validate_temporal_defaults(temporal))

    # Validate raster defaults
    raster = defaults.get("raster")
    if raster is not None:
        if not isinstance(raster, dict):
            errors.append("Field 'defaults.raster' must be a mapping")
        else:
            errors.extend(_validate_raster_defaults(raster))

    return errors


def generate_metadata_template() -> str:
    """Generate a metadata.yaml template with comments.

    Returns a YAML string with required and optional fields,
    with comments explaining each field's purpose.

    Note: title and description are NOT included - they come from STAC
    (set during catalog/collection init).

    Returns:
        YAML template string ready to write to file.
    """
    return """# .portolan/metadata.yaml
#
# Human-enrichable metadata that supplements STAC.
# Title and description come from your catalog/collection STAC metadata.
# Columns and bands are auto-extracted from data files.
#
# Only contact and license are REQUIRED here.

# -----------------------------------------------------------------------------
# REQUIRED: Accountability
# -----------------------------------------------------------------------------

contact:
  name: ""                          # Person or team name
  email: ""                         # Contact email

license: ""                         # SPDX identifier (e.g., "CC-BY-4.0", "MIT")

# -----------------------------------------------------------------------------
# OPTIONAL: Discovery and citation
# -----------------------------------------------------------------------------

license_url: ""                     # optional - URL to full license text
citation: ""                        # optional - Academic citation text
doi: ""                             # optional - Zenodo/DataCite DOI
keywords: []                        # optional - Discovery tags
attribution: ""                     # optional - Required attribution text for maps

# -----------------------------------------------------------------------------
# OPTIONAL: Data lifecycle
# -----------------------------------------------------------------------------

source_url: ""                      # optional - Original data source URL
processing_notes: ""                # optional - How data was processed/transformed
known_issues: ""                    # optional - Known limitations or caveats

# -----------------------------------------------------------------------------
# OPTIONAL: Data defaults (when auto-extraction fails or needs override)
# These values apply to items where the source file lacks the metadata.
# Useful for datasets where nodata or temporal info wasn't set upstream.
# -----------------------------------------------------------------------------

# defaults:
#   temporal:
#     year: 2025                    # Year range (Jan 1 - Dec 31)
#     # Or explicit bounds:
#     # start: "2025-04-15"         # ISO date (YYYY-MM-DD)
#     # end: "2025-05-30"
#   raster:
#     nodata: 0                     # Uniform nodata for all bands
#     # Or per-band:
#     # nodata: [0, 0, 255]         # Per-band nodata values
"""


def apply_temporal_defaults(
    defaults: dict[str, Any],
) -> datetime | None:
    """Apply temporal defaults from metadata.yaml.

    Returns a datetime to use for items that don't have explicit datetime.
    Year takes precedence over start date if both are specified.

    Args:
        defaults: The 'defaults' section from metadata.yaml.

    Returns:
        A datetime object or None if no temporal defaults specified.

    Raises:
        ValueError: If date format is invalid (should be caught by validation).
    """
    temporal = defaults.get("temporal")
    if not temporal:
        return None

    # Year takes precedence - produces Jan 1 of that year
    year = temporal.get("year")
    if year is not None:
        if not isinstance(year, int):
            raise ValueError(f"Year must be an integer, got {type(year).__name__}")
        return datetime(year, 1, 1, tzinfo=timezone.utc)

    # Fall back to start date
    start = temporal.get("start")
    if start is not None:
        try:
            # Use date.fromisoformat for safe parsing (validates month/day)
            parsed_date = date.fromisoformat(start)
            return datetime(
                parsed_date.year,
                parsed_date.month,
                parsed_date.day,
                tzinfo=timezone.utc,
            )
        except ValueError as e:
            raise ValueError(f"Invalid date format for start: '{start}'. {e}") from e

    return None


class NodataMismatchError(ValueError):
    """Raised when per-band nodata list length doesn't match band count."""

    pass


def apply_raster_nodata_defaults(
    defaults: dict[str, Any],
    nodatavals: tuple[float | None, ...] | None,
    band_count: int,
    *,
    strict: bool = True,
) -> tuple[float | None, ...]:
    """Apply raster nodata defaults from metadata.yaml.

    Fills in missing nodata values from defaults. Existing values are preserved.

    Args:
        defaults: The 'defaults' section from metadata.yaml.
        nodatavals: Current nodata values tuple (may be None or contain Nones).
        band_count: Number of bands in the raster.
        strict: If True (default), raise error on per-band list length mismatch.
            If False, log a warning and pad with last value.

    Returns:
        Updated nodatavals tuple with defaults applied.

    Raises:
        NodataMismatchError: If strict=True and per-band nodata list length
            doesn't match band_count.
    """
    raster = defaults.get("raster")
    if not raster or "nodata" not in raster:
        # No raster defaults - return original or tuple of Nones
        if nodatavals is None:
            return tuple(None for _ in range(band_count))
        return nodatavals

    default_nodata = raster["nodata"]

    # Check for per-band nodata length mismatch
    if isinstance(default_nodata, list) and len(default_nodata) != band_count:
        msg = (
            f"Per-band nodata list has {len(default_nodata)} values but raster has "
            f"{band_count} bands. Either use uniform nodata (single value) or provide "
            f"exactly {band_count} values."
        )
        if strict:
            raise NodataMismatchError(msg)
        else:
            logger.warning(
                "%s Padding with last value (%s) for remaining bands.",
                msg,
                default_nodata[-1] if default_nodata else "None",
            )

    # Handle None input
    if nodatavals is None:
        nodatavals = tuple(None for _ in range(band_count))

    result: list[float | None] = []
    for i in range(band_count):
        existing = nodatavals[i] if i < len(nodatavals) else None

        if existing is not None:
            # Preserve existing nodata (ensure float for type consistency)
            result.append(float(existing))
        elif isinstance(default_nodata, list):
            # Per-band defaults
            if i < len(default_nodata):
                val = default_nodata[i]
                result.append(float(val) if val is not None else None)
            else:
                # Pad with last value (only reached if strict=False)
                last_val = default_nodata[-1] if default_nodata else None
                result.append(float(last_val) if last_val is not None else None)
        else:
            # Uniform default (ensure float for type consistency)
            result.append(float(default_nodata) if default_nodata is not None else None)

    return tuple(result)


def load_and_validate_metadata(
    path: Path,
    catalog_root: Path,
) -> tuple[dict[str, Any], list[str]]:
    """Load metadata from hierarchy and validate.

    Uses hierarchical .portolan/ resolution to merge metadata.yaml files
    from catalog_root down to path, then validates the merged result.

    Args:
        path: Directory to start from (collection or subcatalog).
        catalog_root: Catalog root directory.

    Returns:
        Tuple of (merged_metadata_dict, list_of_errors).
        Returns ({}, errors) if no metadata.yaml exists in hierarchy.
    """
    # Load merged metadata using existing hierarchy support
    metadata = load_merged_metadata(path, catalog_root)

    # Validate the merged result
    errors = validate_metadata(metadata)

    return metadata, errors
