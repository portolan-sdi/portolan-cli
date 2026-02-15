"""ItemModel dataclass for STAC Item metadata.

An Item represents a single dataset within a collection.
It has geometry, bbox, datetime, and assets per the STAC spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from portolan_cli.models.catalog import Link


@dataclass
class AssetModel:
    """A STAC asset (file reference).

    Attributes:
        href: Asset URL or relative path.
        type: Media type (e.g., "application/x-parquet").
        roles: Asset roles (e.g., ["data"], ["thumbnail"]).
        title: Human-readable title.
    """

    href: str
    type: str | None = None
    roles: list[str] | None = None
    title: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {"href": self.href}
        if self.type is not None:
            result["type"] = self.type
        if self.roles is not None:
            result["roles"] = self.roles
        if self.title is not None:
            result["title"] = self.title
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AssetModel:
        """Create AssetModel from dict."""
        return cls(
            href=data["href"],
            type=data.get("type"),
            roles=data.get("roles"),
            title=data.get("title"),
        )


@dataclass
class ItemModel:
    """STAC Item metadata model.

    An Item is a single dataset within a collection.

    Attributes:
        id: Unique item identifier.
        geometry: GeoJSON geometry (bounding polygon).
        bbox: Bounding box [west, south, east, north].
        properties: STAC properties including datetime.
        assets: Asset references keyed by asset name.
        collection: Parent collection ID.
        type: Always "Feature".
        stac_version: STAC spec version ("1.0.0").
        title: Human-readable title (optional).
        description: Item description (optional).
        links: STAC links.
    """

    id: str
    geometry: dict[str, Any] | None
    bbox: list[float]
    properties: dict[str, Any]
    assets: dict[str, AssetModel]
    collection: str
    type: str = field(default="Feature", init=False)
    stac_version: str = field(default="1.0.0", init=False)
    title: str | None = None
    description: str | None = None
    links: list[Link] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate fields after initialization."""
        if len(self.bbox) not in (4, 6):
            raise ValueError(f"bbox must have 4 or 6 elements, got {len(self.bbox)}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "type": self.type,
            "stac_version": self.stac_version,
            "id": self.id,
            "geometry": self.geometry,
            "bbox": self.bbox,
            "properties": self.properties,
            "links": [link.to_dict() for link in self.links],
            "assets": {name: asset.to_dict() for name, asset in self.assets.items()},
            "collection": self.collection,
        }
        if self.title is not None:
            result["title"] = self.title
        if self.description is not None:
            result["description"] = self.description
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ItemModel:
        """Create ItemModel from dict."""
        assets = {
            name: AssetModel.from_dict(asset_data)
            for name, asset_data in data.get("assets", {}).items()
        }
        links = [Link.from_dict(link) for link in data.get("links", [])]

        return cls(
            id=data["id"],
            geometry=data.get("geometry"),
            bbox=data["bbox"],
            properties=data["properties"],
            assets=assets,
            collection=data["collection"],
            title=data.get("title"),
            description=data.get("description"),
            links=links,
        )
