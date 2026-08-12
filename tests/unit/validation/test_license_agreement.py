"""``metadata validate`` and ``check`` must accept the same license identifiers.

Issue #727: ``metadata_yaml`` kept a hand-written 26-identifier subset while
rashid's PTL-LIC-001 validated against its full list. The two disagreed in both
directions on the same file, so a user was told a real license was invalid, or
shipped a catalog that failed validation after ``metadata validate`` passed.

These tests assert the two now agree by construction. They compare against
``rashid.api.SPDX_LICENSE_IDS``, the object the rule itself uses, so a future
divergence has nowhere to hide.
"""

from __future__ import annotations

import random

import pytest
from rashid.api import SPDX_LICENSE_IDS

from portolan_cli.metadata_yaml import validate_metadata

pytestmark = pytest.mark.unit

CONTACT = {"name": "Data Team", "email": "data@example.org"}

#: Enough of the 727 to catch a subset regression without 727 validate calls.
SAMPLE_SIZE = 60


def _license_errors(license_id: str, **extra: str) -> list[str]:
    metadata: dict[str, object] = {"contact": CONTACT, "license": license_id, **extra}
    return [e for e in validate_metadata(metadata) if "license" in e.lower()]


def _sample() -> list[str]:
    """A stable sample, so a failure names the same identifiers on every run."""
    return random.Random(727).sample(sorted(SPDX_LICENSE_IDS), SAMPLE_SIZE)


@pytest.mark.parametrize("license_id", _sample())
def test_every_sampled_spdx_identifier_is_accepted(license_id: str) -> None:
    assert _license_errors(license_id) == []


def test_the_sample_reaches_past_the_old_26_entry_subset() -> None:
    """A sample drawn only from the popular ids would prove nothing.

    The removed subset held 26 identifiers. This asserts the sample is mostly
    outside it, so the parametrized test above would have been red before #727.
    """
    old_subset = {
        "CC0-1.0",
        "CC-BY-4.0",
        "CC-BY-SA-4.0",
        "CC-BY-NC-4.0",
        "CC-BY-NC-SA-4.0",
        "CC-BY-ND-4.0",
        "CC-BY-NC-ND-4.0",
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
        "PDDL-1.0",
        "ODbL-1.0",
        "ODC-By-1.0",
        "CC-PDDC",
    }

    assert len(set(_sample()) - old_subset) >= SAMPLE_SIZE - 5


def test_the_stac_escape_hatch_is_accepted_alongside_a_link() -> None:
    """'other' is not in the SPDX list, so it is allowed separately."""
    assert _license_errors("other", license_url="https://example.org/terms.html") == []


@pytest.mark.parametrize(
    "license_id",
    ["Apache 2.0", "cc-by-4.0", "LicenseRef-CityOfPhiladelphia", "NOT-A-REAL-LICENSE"],
)
def test_a_value_outside_the_shared_list_is_rejected(license_id: str) -> None:
    """Each of these fires PTL-LIC-001 under ``portolan check``."""
    assert license_id not in SPDX_LICENSE_IDS
    assert _license_errors(license_id) != []
