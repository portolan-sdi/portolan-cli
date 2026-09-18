"""Server provider protocol."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from portolan_cli.server.model import PublishPlan, PublishResult


class ServerProvider(Protocol):
    """Protocol implemented by server providers."""

    def plan(self, catalog_root: Path) -> PublishPlan:
        """Plan changes for a Portolan catalog."""

    def publish(self, catalog_root: Path) -> PublishResult:
        """Publish a Portolan catalog."""

    def sync(self, catalog_root: Path) -> PublishResult:
        """Reconcile a Portolan catalog."""
