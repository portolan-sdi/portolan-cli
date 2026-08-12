"""The license a collection must carry before Portolan will write it (issue #686).

A license is a legal fact about someone else's data, so Portolan cannot invent
one. What it can do is refuse to write a collection it already knows the
validator will reject. Two of rashid's three license rules describe failures
generation causes on its own:

- PTL-LIC-001 fires when a collection declares no license, which is what happens
  when metadata.yaml omits the field and the collection keeps ``DEFAULT_LICENSE``;
- PTL-LIC-002 fires when the license is ``other`` and no ``rel="license"`` link
  points at the license text, which is what happens when metadata.yaml writes
  ``license: other`` and no ``license_url``.

``license_gap`` answers whether either is about to happen, branch for branch
against ``rashid.rules.license``, so generation and validation never disagree.
It also catches the seeded TODO placeholder, which reaches ``collection.license``
verbatim, and the deprecated ``proprietary`` value (PTL-LIC-003).

Spelling is not judged here. rashid keeps its 727-identifier SPDX list in the
private ``rashid._spdx``, which this package may not import, so an identifier
this module waves through can still be misspelled. ``portolan check`` owns that
verdict, and duplicating a list that long would only let the two copies drift.

``license_url_from_text`` reads a license link back out of raw upstream license
text. ArcGIS ``licenseInfo`` and ISO access constraints usually carry one, and a
URL the source published is a real license link rather than a guess.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from portolan_cli.constants import TODO_MARKER
from portolan_cli.errors import MissingLicenseError

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
    """Read the license a human wrote in merged metadata.yaml.

    Reading and judging stay separate: a TODO placeholder or ``proprietary``
    resolves fine here and fails ``license_gap``. That split lets a caller tell
    "the human wrote nothing, fall back to the existing collection" apart from
    "the human wrote something unusable, stop".

    Args:
        metadata: Merged metadata.yaml mapping (other types yield None).

    Returns:
        The declared license, or None when the metadata declares no license.
    """
    if not isinstance(metadata, Mapping):
        return None

    license_id = _text(metadata.get("license"))
    if license_id is None:
        return None

    return ResolvedLicense(license_id=license_id, license_url=_text(metadata.get("license_url")))


def license_gap(license_id: str | None, *, has_license_link: bool) -> str | None:
    """Return why this license fails the validator, or None when it passes.

    Mirrors ``rashid.rules.license`` branch for branch, so a collection this
    function waves through is a collection ``portolan check`` accepts on the
    license rules. Identifier spelling is out of scope, per the module docstring.

    Args:
        license_id: The value headed for ``collection.license``.
        has_license_link: Whether the collection will carry a ``rel="license"``
            link, from either ``license_url`` or a link already on disk.

    Returns:
        A reason phrased to complete "no usable license: ...", or None.
    """
    declared = _text(license_id)
    if declared is None:
        return "no license is declared"

    if declared.upper().startswith("TODO"):
        return f"license is still the seeded {TODO_MARKER!r} placeholder"

    if declared == PROPRIETARY_LICENSE:
        return "'proprietary' is deprecated and must not be used"

    if declared == OTHER_LICENSE and not has_license_link:
        return "license 'other' needs a license_url pointing at the license text"

    return None


def resolve_harvest_license(
    *,
    cli_license: str | None,
    cli_license_url: str | None,
    harvested_license_url: str | None,
) -> ResolvedLicense:
    """Decide the license an extraction will seed, before it downloads anything.

    What the human passed wins, because they can name an SPDX identifier and the
    harvest never can. Failing that, a licence URL the source published supports
    ``other`` plus a link, which is conformant and states only what the source
    states. With neither, there is nothing honest to seed, and stopping here costs
    the user a re-run of the command rather than a re-run of the download.

    Args:
        cli_license: Value of --license, or None.
        cli_license_url: Value of --license-url, or None.
        harvested_license_url: URL found in the source's own license text, or None.

    Returns:
        The license to seed into metadata.yaml.

    Raises:
        MissingLicenseError: When neither source yields a conformant license.
    """
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
    """Pull the license link out of raw upstream license text.

    Sources describe their licence in prose or in an HTML fragment, and the URL
    inside it is the license text the spec asks a ``rel="license"`` link to point
    at. Text with no URL yields None, which leaves the human to supply one rather
    than inventing a link to a page that says nothing about licensing.

    Args:
        text: Raw license text as harvested, such as ArcGIS ``licenseInfo``.

    Returns:
        The first URL in the text, or None when it holds none.
    """
    if not text:
        return None

    match = _URL_PATTERN.search(text)
    if match is None:
        return None

    return match.group().rstrip(_URL_TRAILING) or None
