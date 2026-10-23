"""Unit tests for ImageServer extraction modules."""

from __future__ import annotations

import pytest


def _lerc_missing() -> str | None:
    """Return why the LERC decoder cannot load here, or None when it loads.

    The ``lerc`` wheel ships no aarch64 build of ``libLerc.so``, so the import
    fails on that platform. CI runs on x86, where the tests run.
    """
    try:
        import lerc  # type: ignore[import-untyped]  # noqa: F401
    except (ImportError, OSError) as err:
        return f"the LERC decoder does not load on this platform: {err}"
    return None


_LERC_MISSING = _lerc_missing()

#: Skip a test that decodes a LERC tile when the decoder cannot load.
requires_lerc = pytest.mark.skipif(_LERC_MISSING is not None, reason=_LERC_MISSING or "")
