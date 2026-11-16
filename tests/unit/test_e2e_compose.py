"""Regression tests for the image pins in the e2e Docker stack.

MinIO withdrew its community images in September 2026 and every branch failed
at the image pull (#919). A digest pin makes a re-pushed tag fail loudly instead
of changing the stack. Renovate proposes each new digest as a pull request. The
org preset in portolan-sdi/portolan-ops (renovate/default.json5) scans this test
directory, so the repo holds no update config of its own.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "tests" / "iceberg" / "e2e" / "docker-compose.yml"

PINNED_IMAGE = re.compile(r"^[a-z0-9./-]+:[\w.-]+@sha256:[0-9a-f]{64}$")


def _load(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


def test_every_e2e_image_is_pinned_by_tag_and_digest() -> None:
    """Each service names a readable tag and the digest that the tag resolved to."""
    services = _load(COMPOSE_FILE)["services"]

    unpinned = {
        name: service["image"]
        for name, service in services.items()
        if not PINNED_IMAGE.match(service["image"])
    }

    assert set(services) == {"rest-catalog", "rustfs", "rustfs-init"}
    assert unpinned == {}
