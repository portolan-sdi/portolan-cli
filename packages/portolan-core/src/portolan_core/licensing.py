"""The license a collection must carry before Portolan will write it."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from rashid_core.api import SPDX_LICENSE_IDS, canonical_spdx_id

from portolan_core.constants import TODO_MARKER
from portolan_core.errors import MissingLicenseError

#: The link relation that satisfies PTL-LIC-002.
LICENSE_REL = "license"

#: STAC's escape hatch for a license with no SPDX identifier. Conformant only
#: alongside a ``rel="license"`` link.
OTHER_LICENSE = "other"

#: Deprecated by the spec and rejected outright by PTL-LIC-003.
PROPRIETARY_LICENSE = "proprietary"

#: What to tell a human whose metadata.yaml carries no usable license. Names both
#: conformant shapes, so fixing the error does not require reading the spec.
METADATA_LICENSE_REMEDIATION = (
    "Set 'license:' in .portolan/metadata.yaml to an SPDX identifier such as "
    "CC-BY-4.0, or to 'other' with a 'license_url:' pointing at the license text."
)

#: The same advice for a command that takes the license as a flag.
CLI_LICENSE_REMEDIATION = (
    "Pass --license with an SPDX identifier such as CC-BY-4.0, or --license other "
    "together with --license-url pointing at the license text."
)

_URL_PATTERN = re.compile(r"https?://[^\s<>\"'`)\]}]+")

#: Trailing characters a URL picks up from prose or markup but does not own.
_URL_TRAILING = ".,;:!?"


@dataclass(frozen=True)
class ResolvedLicense:
    """A license as metadata.yaml declares it, before anyone judges it."""

    license_id: str
    license_url: str | None


def _text(value: object) -> str | None:
    """Return a stripped non-empty string, or None for anything else."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def resolve_license(metadata: object) -> ResolvedLicense | None:
    """Read the license a human wrote in merged metadata.yaml."""
    if not isinstance(metadata, Mapping):
        return None

    license_id = _text(metadata.get("license"))
    if license_id is None:
        return None

    return ResolvedLicense(license_id=license_id, license_url=_text(metadata.get("license_url")))


def license_gap(license_id: str | None, *, has_license_link: bool) -> str | None:
    """Return why this license fails the validator, or None when it passes."""
    declared = _text(license_id)
    if declared is None:
        return "no license is declared"

    if declared.upper().startswith("TODO"):
        return f"license is still the seeded {TODO_MARKER!r} placeholder"

    if declared == PROPRIETARY_LICENSE:
        return "'proprietary' is deprecated and must not be used"

    if declared == OTHER_LICENSE:
        if has_license_link:
            return None
        return "license 'other' needs a license_url pointing at the license text"

    if declared in SPDX_LICENSE_IDS:
        return None

    # A near miss earns the official spelling, the same hint PTL-LIC-001 gives.
    canonical = canonical_spdx_id(declared)
    if canonical is not None:
        return (
            f"license {declared!r} is misspelled; SPDX ids are case-sensitive, write {canonical!r}"
        )

    return f"license {declared!r} is not an SPDX identifier; use one, or 'other' with a license_url"


def resolve_harvest_license(
    *,
    cli_license: str | None,
    cli_license_url: str | None,
    harvested_license_url: str | None,
) -> ResolvedLicense:
    """Decide the license an extraction will seed, before it downloads anything."""
    declared = (cli_license or "").strip()
    if declared:
        resolved = ResolvedLicense(declared, (cli_license_url or "").strip() or None)
    elif harvested_license_url:
        resolved = ResolvedLicense(OTHER_LICENSE, harvested_license_url)
    else:
        raise MissingLicenseError(
            "this extraction",
            "the source publishes no license URL to link to",
        )

    gap = license_gap(resolved.license_id, has_license_link=resolved.license_url is not None)
    if gap is not None:
        raise MissingLicenseError("this extraction", gap)

    return resolved


def license_url_from_text(text: str | None) -> str | None:
    """Pull the license link out of raw upstream license text."""
    if not text:
        return None

    match = _URL_PATTERN.search(text)
    if match is None:
        return None

    return match.group().rstrip(_URL_TRAILING) or None
