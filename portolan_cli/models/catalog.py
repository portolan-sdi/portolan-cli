"""CatalogModel dataclass for STAC Catalog metadata.

The catalog is the top-level container in a Portolan workspace.
It contains references to collections and follows the STAC Catalog spec.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

# Valid STAC identifier pattern: alphanumeric, hyphens, underscores
ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


@dataclass
class Link:
    """A STAC link object.

    Links connect catalogs, collections, items, and external resources.

    Attributes:
        rel: Link relationship (e.g., "self", "root", "child", "item").
        href: Link URL or relative path.
        type: Media type of linked resource (optional).
        title: Human-readable link title (optional).
    """

    rel: str
    href: str
    type: str | None = None
    title: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns:
            Dict with non-None fields only.
        """
        result: dict[str, Any] = {
            "rel": self.rel,
            "href": self.href,
        }
        if self.type is not None:
            result["type"] = self.type
        if self.title is not None:
            result["title"] = self.title
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Link:
        """Create Link from dict.

        Args:
            data: Dictionary with link fields.

        Returns:
            Link instance.
        """
        return cls(
            rel=data["rel"],
            href=data["href"],
            type=data.get("type"),
            title=data.get("title"),
        )


@dataclass
class CatalogModel:
    """STAC Catalog metadata model.

    A Catalog is the top-level container in a Portolan workspace.
    It references collections and provides metadata about the catalog itself.

    Attributes:
        id: Unique identifier (auto-extracted from directory name).
        description: Catalog description (required by STAC).
        type: Always "Catalog".
        stac_version: STAC spec version ("1.0.0").
        title: Human-readable title (optional best practice).
        created: Creation timestamp (auto-generated).
        updated: Last update timestamp (auto-generated).
        links: STAC links to collections and self.
    """

    id: str
    description: str
    type: str = field(default="Catalog", init=False)
    stac_version: str = field(default="1.0.0", init=False)
    title: str | None = None
    created: datetime | None = None
    updated: datetime | None = None
    links: list[Link] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate fields after initialization."""
        if not ID_PATTERN.match(self.id):
            raise ValueError(f"Invalid catalog id '{self.id}': must match pattern ^[a-zA-Z0-9_-]+$")

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns:
            Dict with all fields, timestamps in ISO 8601 format.
        """
        result: dict[str, Any] = {
            "type": self.type,
            "stac_version": self.stac_version,
            "id": self.id,
            "description": self.description,
            "links": [link.to_dict() for link in self.links],
        }
        if self.title is not None:
            result["title"] = self.title
        if self.created is not None:
            result["created"] = self.created.isoformat()
        if self.updated is not None:
            result["updated"] = self.updated.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CatalogModel:
        """Create CatalogModel from dict.

        Args:
            data: Dictionary with catalog fields.

        Returns:
            CatalogModel instance.
        """
        created = None
        if data.get("created"):
            created = datetime.fromisoformat(data["created"])

        updated = None
        if data.get("updated"):
            updated = datetime.fromisoformat(data["updated"])

        links = [Link.from_dict(link) for link in data.get("links", [])]

        return cls(
            id=data["id"],
            description=data["description"],
            title=data.get("title"),
            created=created,
            updated=updated,
            links=links,
        )
