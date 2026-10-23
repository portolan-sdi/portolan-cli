"""The ``example`` versioning backend.

It keeps each collection's history in
``.portolan/example-versions/<collection>.json``. It has no rollback or prune.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from portolan_cli.api import DriftReport, PostAddContext, PullResult, SchemaFingerprint, Version


class ExampleBackend:
    """A versioning backend that stores a version list per collection."""

    def __init__(self, catalog_root: Path | None = None) -> None:
        self._root = (catalog_root or Path.cwd()) / ".portolan" / "example-versions"

    def _path(self, collection: str) -> Path:
        return self._root / f"{collection.replace('/', '__')}.json"

    def _read(self, collection: str) -> list[dict[str, Any]]:
        path = self._path(collection)
        if not path.exists():
            return []
        history: list[dict[str, Any]] = json.loads(path.read_text(encoding="utf-8"))
        return history

    @staticmethod
    def _version(entry: dict[str, Any]) -> Version:
        return Version(
            version=entry["version"],
            created=datetime.fromisoformat(entry["created"]),
            breaking=entry["breaking"],
            assets={},
            changes=entry["changes"],
            message=entry["message"],
        )

    def get_current_version(self, collection: str) -> Version:
        """Return the newest version."""
        history = self._read(collection)
        if not history:
            raise FileNotFoundError(f"No versions for collection '{collection}'.")
        return self._version(history[-1])

    def list_versions(self, collection: str) -> list[Version]:
        """Return every version, oldest first."""
        return [self._version(entry) for entry in self._read(collection)]

    # The protocol fixes this signature, so every backend repeats it.
    # jscpd:ignore-start
    def publish(
        self,
        collection: str,
        assets: dict[str, str],
        schema: SchemaFingerprint,
        breaking: bool,
        message: str,
        removed: set[str] | None = None,
        version: str | None = None,
    ) -> Version:
        # jscpd:ignore-end
        """Append a version. A breaking change raises the major number."""
        history = self._read(collection)
        if version is None:
            major, minor = 1, 0
            if history:
                last_major, last_minor, _ = (int(p) for p in history[-1]["version"].split("."))
                major, minor = (last_major + 1, 0) if breaking else (last_major, last_minor + 1)
            version = f"{major}.{minor}.0"
        entry = {
            "version": version,
            "created": datetime.now(timezone.utc).isoformat(),
            "breaking": breaking,
            "changes": sorted(assets),
            "message": message,
        }
        history.append(entry)
        path = self._path(collection)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(history, indent=2), encoding="utf-8")
        return self._version(entry)

    def rollback(self, collection: str, target_version: str) -> Version:
        """The example backend has no rollback."""
        raise NotImplementedError("The example backend has no rollback.")

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """The example backend has no prune."""
        raise NotImplementedError("The example backend has no prune.")

    def check_drift(self, collection: str) -> DriftReport:
        """The example backend has no remote, so it never drifts."""
        history = self._read(collection)
        current = history[-1]["version"] if history else None
        return DriftReport(
            has_drift=False,
            local_version=current,
            remote_version=current,
            message="The example backend keeps no remote copy.",
        )

    # The four lifecycle hooks of the protocol (#977).
    def on_post_add(self, context: PostAddContext) -> None:
        """Record which items the last add of a collection wrote."""
        record = self._path(context["collection_id"]).with_suffix(".post-add.json")
        record.parent.mkdir(parents=True, exist_ok=True)
        items = [item["item_id"] for item in context["items"]]
        record.write_text(json.dumps(items), encoding="utf-8")

    def pull(
        self,
        remote_url: str,
        local_root: Path,
        collection: str,
        *,
        dry_run: bool = False,
    ) -> PullResult | None:
        """The example backend has no remote, so the file-based pull runs."""
        return None

    def supports_push(self) -> bool:
        """The catalog is plain files, so ``push`` uploads it."""
        return True

    def push_blocked_message(self, remote: str | None) -> str:
        """Never shown, because push is allowed."""
        return "The example backend allows push."
