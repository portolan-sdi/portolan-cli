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

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from portolan_cli.config import load_merged_metadata

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


# =============================================================================
# Validation
# =============================================================================


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

    # Check required fields (contact and license only)
    if "contact" not in metadata:
        errors.append("Required field 'contact' is missing")
    else:
        contact = metadata.get("contact")
        if not isinstance(contact, dict):
            errors.append("Field 'contact' must be a mapping with 'name' and 'email'")
        else:
            for subfield in REQUIRED_CONTACT_FIELDS:
                if subfield not in contact:
                    errors.append(f"Required field 'contact.{subfield}' is missing")
                elif not contact[subfield] or not str(contact[subfield]).strip():
                    errors.append(f"Field 'contact.{subfield}' cannot be empty")
            # Validate email format if present
            email = contact.get("email")
            if email and not EMAIL_PATTERN.match(str(email)):
                errors.append(f"Invalid email format: '{email}'")

    if "license" not in metadata:
        errors.append("Required field 'license' is missing")
    elif not metadata["license"] or not str(metadata["license"]).strip():
        errors.append("Field 'license' cannot be empty")
    else:
        # Validate license is SPDX identifier or valid LicenseRef-* custom identifier
        # Per SPDX spec, LicenseRef-[idstring] is valid for proprietary/custom licenses
        license_id = str(metadata.get("license"))
        is_standard_license = license_id in COMMON_SPDX_LICENSES
        is_custom_license = LICENSEREF_PATTERN.match(license_id) is not None
        if not is_standard_license and not is_custom_license:
            errors.append(
                f"Invalid SPDX license identifier: '{license_id}'. "
                f"Use a standard license (MIT, Apache-2.0, CC-BY-4.0, CC0-1.0) "
                f"or custom format LicenseRef-YourLicense"
            )

    # Validate DOI format if present (optional field)
    doi = metadata.get("doi")
    if doi and str(doi).strip():
        if not DOI_PATTERN.match(str(doi)):
            errors.append(
                f"Invalid DOI format: '{doi}'. DOIs should be like '10.5281/zenodo.1234567'"
            )

    # Validate defaults section if present (optional)
    defaults = metadata.get("defaults")
    if defaults is not None:
        if not isinstance(defaults, dict):
            errors.append("Field 'defaults' must be a mapping")
        else:
            errors.extend(_validate_defaults(defaults))

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
            # Validate year (must be integer)
            year = temporal.get("year")
            if year is not None and not isinstance(year, int):
                errors.append(
                    f"Field 'defaults.temporal.year' must be an integer, got {type(year).__name__}"
                )

            # Validate start/end dates (must be ISO format YYYY-MM-DD)
            for field in ("start", "end"):
                date_val = temporal.get(field)
                if date_val is not None:
                    if not isinstance(date_val, str):
                        errors.append(
                            f"Field 'defaults.temporal.{field}' must be a date string (YYYY-MM-DD)"
                        )
                    elif not ISO_DATE_PATTERN.match(date_val):
                        errors.append(
                            f"Invalid date format for 'defaults.temporal.{field}': '{date_val}'. "
                            f"Use ISO format YYYY-MM-DD"
                        )

    # Validate raster defaults
    raster = defaults.get("raster")
    if raster is not None:
        if not isinstance(raster, dict):
            errors.append("Field 'defaults.raster' must be a mapping")
        else:
            # Validate nodata (must be number or list of numbers)
            nodata = raster.get("nodata")
            if nodata is not None:
                if isinstance(nodata, list):
                    # Per-band nodata: must be list of numbers
                    for i, val in enumerate(nodata):
                        if not isinstance(val, (int, float)) and val is not None:
                            errors.append(
                                f"Field 'defaults.raster.nodata[{i}]' must be a number, "
                                f"got {type(val).__name__}"
                            )
                elif not isinstance(nodata, (int, float)):
                    errors.append(
                        f"Field 'defaults.raster.nodata' must be a number or list of numbers, "
                        f"got {type(nodata).__name__}"
                    )

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
    """
    temporal = defaults.get("temporal")
    if not temporal:
        return None

    # Year takes precedence - produces Jan 1 of that year
    year = temporal.get("year")
    if year is not None:
        return datetime(year, 1, 1, tzinfo=timezone.utc)

    # Fall back to start date
    start = temporal.get("start")
    if start is not None:
        # Parse YYYY-MM-DD
        parts = start.split("-")
        return datetime(int(parts[0]), int(parts[1]), int(parts[2]), tzinfo=timezone.utc)

    return None


def apply_raster_nodata_defaults(
    defaults: dict[str, Any],
    nodatavals: tuple[float | None, ...] | None,
    band_count: int,
) -> tuple[float | None, ...]:
    """Apply raster nodata defaults from metadata.yaml.

    Fills in missing nodata values from defaults. Existing values are preserved.

    Args:
        defaults: The 'defaults' section from metadata.yaml.
        nodatavals: Current nodata values tuple (may be None or contain Nones).
        band_count: Number of bands in the raster.

    Returns:
        Updated nodatavals tuple with defaults applied.
    """
    raster = defaults.get("raster")
    if not raster or "nodata" not in raster:
        # No raster defaults - return original or tuple of Nones
        if nodatavals is None:
            return tuple(None for _ in range(band_count))
        return nodatavals

    default_nodata = raster["nodata"]

    # Handle None input
    if nodatavals is None:
        nodatavals = tuple(None for _ in range(band_count))

    result: list[float | None] = []
    for i in range(band_count):
        existing = nodatavals[i] if i < len(nodatavals) else None

        if existing is not None:
            # Preserve existing nodata
            result.append(existing)
        elif isinstance(default_nodata, list):
            # Per-band defaults - use last value if list is shorter
            if i < len(default_nodata):
                result.append(default_nodata[i])
            else:
                result.append(default_nodata[-1] if default_nodata else None)
        else:
            # Uniform default
            result.append(default_nodata)

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
