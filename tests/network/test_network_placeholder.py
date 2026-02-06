"""Network tests that hit live services.

Run nightly in CI, mocked locally. Add real tests as remote sync features are built.
"""

from __future__ import annotations

import urllib.request

import pytest


@pytest.mark.network
def test_can_reach_pypi() -> None:
    """Verify network connectivity by checking PyPI is reachable.

    This placeholder confirms the network test infrastructure works.
    Replace with real tests (e.g., S3 connectivity) as features are added.
    """
    # Simple HEAD request to PyPI - lightweight, always available
    req = urllib.request.Request("https://pypi.org/", method="HEAD")
    with urllib.request.urlopen(req, timeout=10) as response:
        assert response.status == 200
