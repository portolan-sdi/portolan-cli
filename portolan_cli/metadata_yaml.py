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
"""


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
