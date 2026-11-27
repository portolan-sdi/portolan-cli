"""Metadata YAML schema and validation."""

from __future__ import annotations

import logging
import math
import re
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from portolan_cli.providers import HOST_ROLE, PROVIDER_ROLES
from portolan_core.config import load_merged_metadata
from portolan_core.licensing import OTHER_LICENSE, license_gap

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_core.config import ConfigSource

_MIN_YEAR = 1800
_MAX_YEAR = 2100

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = frozenset({"contact", "license"})
REQUIRED_CONTACT_FIELDS = frozenset({"name", "email"})

# Basic email pattern - not RFC 5322 compliant but catches obvious errors
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# DOI: 10.XXXX/suffix with 4+ digits. https://www.doi.org/doi_handbook/2_Numbering.html
DOI_PATTERN = re.compile(r"^10\.\d{4,}/\S+$")

# ISO date pattern: YYYY-MM-DD
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# ORCID: four groups of 4, last char may be X. https://support.orcid.org/hc/en-us/articles/360006897674
ORCID_PATTERN = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")

# URL pattern: Basic validation for http/https URLs
# Not comprehensive, but catches obviously malformed URLs
URL_PATTERN = re.compile(r"^https?://[^\s<>\"{}|\\^`\[\]]+$")


def _validate_authors(authors: Any) -> list[str]:
    """Validate the 'authors' field (list of author dicts)."""
    errors: list[str] = []

    if not isinstance(authors, list):
        errors.append("Field 'authors' must be a list")
        return errors

    for i, author in enumerate(authors):
        if author is None:
            errors.append(f"Author entry {i} is None (expected a mapping with 'name' field)")
            continue
        if not isinstance(author, dict):
            errors.append(f"Author entry {i} must be a mapping with 'name' field")
            continue

        # Name is required and must be a non-empty string
        if "name" not in author:
            errors.append(f"Author entry {i} is missing required 'name' field")
        elif not isinstance(author["name"], str):
            errors.append(
                f"Author entry {i} 'name' must be a string, got {type(author['name']).__name__}"
            )
        elif not author["name"].strip():
            errors.append(f"Author entry {i} has empty 'name' field")

        # Validate ORCID format if present
        orcid = author.get("orcid")
        if orcid and str(orcid).strip() and not ORCID_PATTERN.match(str(orcid)):
            errors.append(
                f"Author entry {i} has invalid ORCID format: '{orcid}'. "
                f"ORCIDs should be like '0000-0001-2345-6789'"
            )

        # Validate email format if present
        email = author.get("email")
        if email and str(email).strip() and not EMAIL_PATTERN.match(str(email)):
            errors.append(f"Author entry {i} has invalid email format: '{email}'")

    return errors


def _validate_related_dois(related_dois: Any) -> list[str]:
    """Validate the 'related_dois' field (list of DOI strings)."""
    errors: list[str] = []

    if not isinstance(related_dois, list):
        errors.append("Field 'related_dois' must be a list")
        return errors

    for i, doi in enumerate(related_dois):
        if not isinstance(doi, str):
            errors.append(f"Item {i} in 'related_dois' must be a string")
            continue
        if not doi.strip():
            errors.append(f"Item {i} in 'related_dois' cannot be empty")
            continue
        if not DOI_PATTERN.match(doi):
            errors.append(
                f"Invalid DOI format in 'related_dois[{i}]': '{doi}'. "
                f"DOIs should be like '10.5281/zenodo.1234567'"
            )

    return errors


def _validate_citations(citations: Any) -> list[str]:
    """Validate the 'citations' field (list of citation strings)."""
    errors: list[str] = []

    if not isinstance(citations, list):
        errors.append("Field 'citations' must be a list of strings")
        return errors

    for i, citation in enumerate(citations):
        if not isinstance(citation, str):
            errors.append(f"Citation entry {i} must be a string")
        elif not citation.strip():
            errors.append(f"Citation entry {i} cannot be empty or whitespace")

    return errors


def _validate_upstream_version(upstream_version: Any) -> list[str]:
    """Validate the 'upstream_version' field (string)."""
    errors: list[str] = []

    if not isinstance(upstream_version, str):
        errors.append(
            f"Field 'upstream_version' must be a string, got {type(upstream_version).__name__}"
        )

    return errors


def _validate_upstream_version_url(upstream_version_url: Any) -> list[str]:
    """Validate the 'upstream_version_url' field (URL string)."""
    errors: list[str] = []

    if not isinstance(upstream_version_url, str):
        errors.append(
            f"Field 'upstream_version_url' must be a string, got {type(upstream_version_url).__name__}"
        )
        return errors

    if not upstream_version_url.strip():
        errors.append("Field 'upstream_version_url' cannot be empty")
        return errors

    if not URL_PATTERN.match(upstream_version_url):
        errors.append(
            f"Field 'upstream_version_url' must be a valid http/https URL, got '{upstream_version_url}'"
        )

    return errors


def _validate_contact(metadata: dict[str, Any]) -> list[str]:
    """Validate the required 'contact' field."""
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
    """Validate the required 'license' field."""
    errors: list[str] = []

    if "license" not in metadata:
        errors.append("Required field 'license' is missing")
        return errors

    if not metadata["license"] or not str(metadata["license"]).strip():
        errors.append("Field 'license' cannot be empty")
        return errors

    license_id = str(metadata["license"])
    gap = license_gap(license_id, has_license_link=bool(metadata.get("license_url")))
    if gap is None:
        return errors

    # This function reads metadata.yaml alone, so it cannot see a rel="license"
    # link already written to collection.json. Name that escape as well.
    if license_id == OTHER_LICENSE:
        gap += ', or a rel="license" link already on the collection'
    errors.append(f"Invalid license: {gap}")

    return errors


def _validate_provider_roles(roles: Any, index: int) -> list[str]:
    """Validate one provider's roles against the four STAC defines."""
    if not isinstance(roles, list):
        return [f"Field 'providers[{index}].roles' must be a list"]

    errors: list[str] = [
        f"Field 'providers[{index}].roles' has unknown role '{role}'. "
        f"Use one of {', '.join(PROVIDER_ROLES)}"
        for role in roles
        if not isinstance(role, str) or role not in PROVIDER_ROLES
    ]
    return errors


def _validate_host_contact(host: dict[str, Any], index: int) -> list[str]:
    """The host provider must be reachable: a url or an email, per PORTO-CORE-051."""
    url = host.get("url")
    email = host.get("email")
    has_url = isinstance(url, str) and url.strip()
    has_email = isinstance(email, str) and email.strip()

    if not has_url and not has_email:
        return [
            f"Field 'providers[{index}]' carries the host role, so it needs a 'url' or an 'email'"
        ]
    if has_email and not EMAIL_PATTERN.match(str(email)):
        return [f"Invalid email format in 'providers[{index}].email': '{email}'"]
    return []


def _validate_providers(metadata: dict[str, Any]) -> list[str]:
    """Validate the optional 'providers' array."""
    providers = metadata.get("providers")
    if providers is None:
        return []
    if not isinstance(providers, list):
        return ["Field 'providers' must be a list"]

    errors: list[str] = []
    hosts: list[int] = []

    for index, provider in enumerate(providers):
        if not isinstance(provider, dict):
            errors.append(f"Field 'providers[{index}]' must be a mapping with a 'name'")
            continue

        name = provider.get("name")
        if name is None:
            errors.append(f"Field 'providers[{index}].name' is missing")
        elif not str(name).strip():
            errors.append(f"Field 'providers[{index}].name' cannot be empty")

        roles = provider.get("roles")
        if roles is not None:
            errors.extend(_validate_provider_roles(roles, index))
            if isinstance(roles, list) and HOST_ROLE in roles:
                hosts.append(index)

    if len(hosts) > 1:
        listed = ", ".join(f"providers[{i}]" for i in hosts)
        errors.append(
            f"Exactly one provider may carry the host role, but {len(hosts)} do: {listed}"
        )
    for index in hosts:
        errors.extend(_validate_host_contact(providers[index], index))

    return errors


def _validate_doi(metadata: dict[str, Any]) -> list[str]:
    """Validate the optional 'doi' field format."""
    errors: list[str] = []

    doi = metadata.get("doi")
    if doi and str(doi).strip() and not DOI_PATTERN.match(str(doi)):
        errors.append(f"Invalid DOI format: '{doi}'. DOIs should be like '10.5281/zenodo.1234567'")

    return errors


def _validate_title_description(metadata: dict[str, Any]) -> list[str]:
    """Validate optional title/description overrides."""
    errors: list[str] = []
    for field in ("title", "description"):
        value = metadata.get(field)
        # None or any string (including blank) is acceptable; blank == absent.
        if value is None or isinstance(value, str):
            continue
        errors.append(f"Field '{field}' must be a string when provided")
    return errors


def validate_metadata(metadata: dict[str, Any]) -> list[str]:
    """Validate a metadata dictionary against the schema."""
    errors: list[str] = []

    # Required fields
    errors.extend(_validate_contact(metadata))
    errors.extend(_validate_license(metadata))

    # Optional fields with format validation
    errors.extend(_validate_providers(metadata))
    errors.extend(_validate_doi(metadata))
    errors.extend(_validate_title_description(metadata))

    # Validate defaults section if present (optional)
    defaults = metadata.get("defaults")
    if defaults is not None:
        if not isinstance(defaults, dict):
            errors.append("Field 'defaults' must be a mapping")
        else:
            errors.extend(_validate_defaults(defaults))

    # Validate the optional fields if present
    if (authors := metadata.get("authors")) is not None:
        errors.extend(_validate_authors(authors))

    if (related_dois := metadata.get("related_dois")) is not None:
        errors.extend(_validate_related_dois(related_dois))

    if (citations := metadata.get("citations")) is not None:
        errors.extend(_validate_citations(citations))

    if (upstream_version := metadata.get("upstream_version")) is not None:
        errors.extend(_validate_upstream_version(upstream_version))

    if (upstream_version_url := metadata.get("upstream_version_url")) is not None:
        errors.extend(_validate_upstream_version_url(upstream_version_url))

    return errors


def _validate_temporal_defaults(temporal: dict[str, Any]) -> list[str]:
    """Validate the 'defaults.temporal' section."""
    errors: list[str] = []

    # Validate year (must be integer in reasonable range)
    year = temporal.get("year")
    if year is not None:
        if not isinstance(year, int):
            errors.append(
                f"Field 'defaults.temporal.year' must be an integer, got {type(year).__name__}"
            )
        elif year < _MIN_YEAR or year > _MAX_YEAR:
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
    """Validate a single nodata value."""
    field = f"defaults.raster.nodata[{index}]" if index is not None else "defaults.raster.nodata"

    if val is None:
        return None
    if not isinstance(val, (int, float)):
        return f"Field '{field}' must be a number, got {type(val).__name__}"
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return f"Field '{field}' must be a finite number, not NaN or Infinity"
    return None


def _validate_raster_defaults(raster: dict[str, Any]) -> list[str]:
    """Validate the 'defaults.raster' section."""
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
    """Validate the 'defaults' section of metadata.yaml."""
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


def generate_metadata_template(*, license_id: str = "", license_url: str = "") -> str:
    """Generate a metadata.yaml template with comments."""
    # Comments line up at column 37 in this template; keep both license lines there.
    comment_column = 36
    license_line = f'license: "{license_id}"'.ljust(comment_column) + (
        '# SPDX identifier (e.g., "CC-BY-4.0", "MIT")'
    )
    license_url_line = f'license_url: "{license_url}"'.ljust(comment_column) + (
        "# optional - URL to full license text"
    )

    # Substituted, not an f-string: an f-string reads as string-built SQL (S608).
    return _TEMPLATE.replace("__LICENSE_LINE__", license_line).replace(
        "__LICENSE_URL_LINE__", license_url_line
    )


_TEMPLATE = """# .portolan/metadata.yaml
#
# Human-enrichable metadata that supplements STAC.
# Columns and bands are auto-extracted from data files.
#
# Only contact and license are REQUIRED here.

# -----------------------------------------------------------------------------
# OPTIONAL: Human-readable title / description override (Issue #502)
# Portolan auto-derives a human-readable title from the collection id
# (e.g. "publico_arbolado" -> "Publico Arbolado"). Set these to override it
# with a better name/description; leave blank to keep the auto-derived value.
# At the catalog root they name the catalog itself. A subcatalog names itself
# the same way. They apply to this object alone. A child never inherits a
# title or a description from its parent.
# -----------------------------------------------------------------------------

# title: ""                         # optional - overrides the auto-derived title
# description: ""                    # optional - overrides the auto-derived description

# -----------------------------------------------------------------------------
# REQUIRED: Accountability
# -----------------------------------------------------------------------------

contact:
  name: ""                          # Person or team name
  email: ""                         # Contact email

__LICENSE_LINE__

# -----------------------------------------------------------------------------
# Providers: who made this data, and who maintains this copy of it
#
# Name the organization that originally created the data with the "producer"
# role. Add "licensor" and "processor" where they apply. Roles may be combined,
# so a self-published collection is one entry holding both producer and host.
#
# The "host" is whoever maintains this catalog, not the cloud vendor storing it:
# a catalog on S3 run by a city GIS office lists the office, never AWS. Leave the
# host out and it is taken from `contact` above. Order does not matter; Portolan
# writes the host last, as the spec requires.
#
# Producer and host together decide what kind of catalog this is. Same
# organization means official, this catalog being the data's canonical home.
# Different organizations mean a mirror, and Portolan then links back to
# `source_url` below and records each sync.
# -----------------------------------------------------------------------------

# providers:
#   - name: "National Statistics Institute"
#     roles: ["producer", "licensor"]
#     url: "https://stats.example.org"
#   - name: "City GIS Office"       # omit to derive this from `contact`
#     roles: ["host"]
#     url: "https://gis.example.org"   # a url or an email is required on the host

# -----------------------------------------------------------------------------
# OPTIONAL: Discovery and citation
# -----------------------------------------------------------------------------

__LICENSE_URL_LINE__
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
# Useful for data where nodata or temporal info wasn't set upstream.
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

# -----------------------------------------------------------------------------
# OPTIONAL: Consumption examples
# -----------------------------------------------------------------------------
# Add collection-specific query examples, especially when structure is unusual
# (multiple related files, required joins, non-obvious column meanings).
# Default examples (DuckDB SQL + GeoPandas) are auto-generated if omitted.
#
# examples:
#   - engine: duckdb                # duckdb | python | r | other
#     description: "Join census data with geographic boundaries"
#     code: |
#       SELECT r.*, c.population
#       FROM read_parquet('https://.../radios.parquet') r
#       JOIN read_parquet('https://.../census-data.parquet') c
#         ON r.cod_2022 = c.id_geo
#   - engine: python
#     description: "Load and merge with GeoPandas"
#     code: |
#       import geopandas as gpd
#       import pandas as pd
#       radios = gpd.read_parquet('https://.../radios.parquet')
#       census = pd.read_parquet('https://.../census-data.parquet')
#       merged = radios.merge(census, left_on='cod_2022', right_on='id_geo')
"""


def apply_temporal_defaults(
    defaults: dict[str, Any],
) -> datetime | None:
    """Apply temporal defaults from metadata.yaml."""
    temporal = defaults.get("temporal")
    if not temporal:
        return None

    # Year takes precedence - produces Jan 1 of that year
    year = temporal.get("year")
    if year is not None:
        if not isinstance(year, int):
            raise ValueError(f"Year must be an integer, got {type(year).__name__}")
        return datetime(year, 1, 1, tzinfo=UTC)

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
                tzinfo=UTC,
            )
        except ValueError as e:
            raise ValueError(f"Invalid date format for start: '{start}'. {e}") from e

    return None


class NodataMismatchError(ValueError):
    """Raised when per-band nodata list length doesn't match band count."""


def apply_raster_nodata_defaults(
    defaults: dict[str, Any],
    nodatavals: tuple[float | None, ...] | None,
    band_count: int,
    *,
    strict: bool = True,
) -> tuple[float | None, ...]:
    """Apply raster nodata defaults from metadata.yaml."""
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
    *,
    state: ConfigSource | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Load metadata from hierarchy and validate."""
    # Load merged metadata using existing hierarchy support
    metadata = (
        state.metadata(path) if state is not None else load_merged_metadata(path, catalog_root)
    )

    # Validate the merged result
    errors = validate_metadata(metadata)

    return metadata, errors
