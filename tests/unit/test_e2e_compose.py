"""Regression tests for the image pins in the e2e Docker stack.

MinIO withdrew its community images in September 2026 and every branch failed
at the image pull (#919). A digest pin makes a re-pushed tag fail loudly instead
of changing the stack, and Dependabot proposes each new digest as a pull request.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = [pytest.mark.unit, pytest.mark.source_scan]

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "tests" / "iceberg" / "e2e" / "docker-compose.yml"
DEPENDABOT_CONFIG = REPO_ROOT / ".github" / "dependabot.yml"

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


def test_dependabot_updates_the_e2e_image_digests() -> None:
    """Dependabot watches the compose file, so the digests do not go stale."""
    updates = _load(DEPENDABOT_CONFIG)["updates"]

    compose_entries = [
        entry
        for entry in updates
        if entry["package-ecosystem"] == "docker-compose"
        and entry["directory"] == "/tests/iceberg/e2e"
    ]

    assert len(compose_entries) == 1
