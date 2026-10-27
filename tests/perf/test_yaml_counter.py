"""Every call of the shared YAML parser contributes to the perf gate."""

from __future__ import annotations

import pytest

from portolan_cli import config, trace

pytestmark = pytest.mark.unit


def test_shared_yaml_helper_counts_every_parse() -> None:
    """An extra parse through the public helper cannot stay invisible."""
    with trace.tracing():
        assert config.safe_load("backend: file") == {"backend": "file"}
        assert config.safe_load("backend: iceberg") == {"backend": "iceberg"}
        assert trace.counters()["config.yaml_parses"] == 2
