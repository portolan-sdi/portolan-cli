"""Translate `.portolan/config.yaml` into rashid's runner configuration.

The `check:` block tunes validation per catalog::

    check:
      disabled:
        - PTL-VIZ-001          # no thumbnails in this catalog, by policy
      severity:
        PTL-TMP-001: error     # temporal extent is mandatory here

    publish:
      public_url: https://data.example.org/my-catalog/

`publish.public_url` is where the catalog is served. It is not a credential —
it is the same URL the catalog's README prints — so it lives in config.yaml
rather than the environment (ADR-0024's sensitive-setting rule does not apply).
`check --live` probes relative asset hrefs against it.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

from rashid.config import RulesConfig

#: Config block replaced by ``check:``. Its keys named native ``RULE-*`` rules
#: that no longer exist, so there is nothing to translate — see ADR-0057.
LEGACY_BLOCK = "stac_lint"


def _load(root: Path) -> dict[str, Any]:
    from portolan_cli.config import load_config

    if not (root / ".portolan" / "config.yaml").exists():
        return {}
    return load_config(root)


def load_rules_config(root: Path) -> RulesConfig:
    """Build rashid's ``RulesConfig`` from the catalog's ``check:`` block.

    Args:
        root: Catalog root (the directory holding ``.portolan/``).

    Returns:
        The configured rule set; an empty ``RulesConfig`` when unconfigured.

    Raises:
        ValueError: A ``severity`` value is not one of error/warning/info.
    """
    config = _load(root)

    if LEGACY_BLOCK in config:
        warnings.warn(
            f"`{LEGACY_BLOCK}:` in .portolan/config.yaml is ignored. Validation moved to "
            "rashid's PTL-* rules (ADR-0057); the old RULE-* names have no equivalent, so "
            "there is nothing to migrate automatically. Re-express the overrides you still "
            "want under `check:` with the PTL-* ids `portolan check` reports.",
            DeprecationWarning,
            stacklevel=2,
        )

    block = config.get("check")
    if not isinstance(block, dict):
        return RulesConfig()

    raw: dict[str, Any] = {}
    disabled = block.get("disabled")
    if isinstance(disabled, list):
        raw["disabled"] = [str(rule_id) for rule_id in disabled]
    severity = block.get("severity")
    if isinstance(severity, dict):
        raw["severity"] = {str(rule_id): str(value) for rule_id, value in severity.items()}

    return RulesConfig.from_dict(raw)


def load_public_url(root: Path) -> str | None:
    """Return the URL the catalog is published under, when configured."""
    publish = _load(root).get("publish")
    if not isinstance(publish, dict):
        return None
    url = publish.get("public_url")
    return url if isinstance(url, str) and url.strip() else None
