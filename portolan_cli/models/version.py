"""VersionModel dataclass for version tracking and change detection.

Versions track schema snapshots and detect breaking changes between releases.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

# Semantic version pattern: X.Y.Z
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


@dataclass
class SchemaFingerprint:
    """Schema snapshot for change detection.

    Captures the structural properties of a schema for comparison.

    Attributes:
        type: Format type ("geoparquet" or "cog").
        fingerprint: Structural schema summary.
    """

    type: str
    fingerprint: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "type": self.type,
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchemaFingerprint:
        """Create SchemaFingerprint from dict."""
        return cls(
            type=data["type"],
            fingerprint=data["fingerprint"],
        )


@dataclass
class AssetVersion:
    """Version-specific asset metadata.

    Attributes:
        sha256: File checksum.
        size_bytes: File size.
        href: Versioned path.
    """

    sha256: str
    size_bytes: int
    href: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "href": self.href,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AssetVersion:
        """Create AssetVersion from dict."""
        return cls(
            sha256=data["sha256"],
            size_bytes=data["size_bytes"],
            href=data["href"],
        )


@dataclass
class VersionModel:
    """Version metadata for a collection.

    Tracks schema fingerprint and detects breaking changes.

    Attributes:
        version: Semantic version string.
        created: Creation timestamp.
        breaking: Whether this version has breaking changes.
        schema: Schema fingerprint for change detection.
        assets: Asset versions with checksums.
        changes: List of changed file paths.
        message: Version description (optional).
    """

    version: str
    created: datetime
    breaking: bool
    schema: SchemaFingerprint
    assets: dict[str, AssetVersion]
    changes: list[str]
    message: str | None = None

    def __post_init__(self) -> None:
        """Validate fields after initialization."""
        if not SEMVER_PATTERN.match(self.version):
            raise ValueError(f"Invalid version '{self.version}': must match semver pattern X.Y.Z")

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "version": self.version,
            "created": self.created.isoformat(),
            "breaking": self.breaking,
            "schema": self.schema.to_dict(),
            "assets": {name: asset.to_dict() for name, asset in self.assets.items()},
            "changes": self.changes,
        }
        if self.message is not None:
            result["message"] = self.message
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VersionModel:
        """Create VersionModel from dict."""
        created = datetime.fromisoformat(data["created"])
        schema = SchemaFingerprint.from_dict(data["schema"])
        assets = {
            name: AssetVersion.from_dict(asset_data)
            for name, asset_data in data.get("assets", {}).items()
        }

        return cls(
            version=data["version"],
            created=created,
            breaking=data["breaking"],
            schema=schema,
            assets=assets,
            changes=data.get("changes", []),
            message=data.get("message"),
        )
