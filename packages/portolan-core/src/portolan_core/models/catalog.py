"""CatalogModel dataclass for STAC Catalog metadata."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from portolan_core.models._stac_version import get_stac_version

# Valid STAC identifier pattern: alphanumeric, hyphens, underscores
ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


@dataclass
class Link:
    """A STAC link object."""

    rel: str
    href: str
    type: str | None = None
    title: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
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
        """Create Link from dict."""
        return cls(
            rel=data["rel"],
            href=data["href"],
            type=data.get("type"),
            title=data.get("title"),
        )


@dataclass
class CatalogModel:
    """STAC Catalog metadata model."""

    id: str
    description: str
    type: str = field(default="Catalog", init=False)
    stac_version: str = field(default_factory=get_stac_version, init=False)
    title: str | None = None
    created: datetime | None = None
    updated: datetime | None = None
    links: list[Link] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate fields after initialization."""
        if not ID_PATTERN.match(self.id):
            raise ValueError(f"Invalid catalog id '{self.id}': must match pattern ^[a-zA-Z0-9_-]+$")

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
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
        """Create CatalogModel from dict."""
        created = None
        if data.get("created"):
            created = datetime.fromisoformat(data["created"])

        updated = None
        if data.get("updated"):
            updated = datetime.fromisoformat(data["updated"])

        links = [Link.from_dict(link) for link in data.get("links", [])]

        catalog = cls(
            id=data["id"],
            description=data["description"],
            title=data.get("title"),
            created=created,
            updated=updated,
            links=links,
        )

        # init=False fields, so set them after construction. Only a valid type
        # applies, so the result stays a valid STAC Catalog.
        if "type" in data and data["type"] == "Catalog":
            object.__setattr__(catalog, "type", data["type"])
        if "stac_version" in data:
            object.__setattr__(catalog, "stac_version", data["stac_version"])

        return catalog
