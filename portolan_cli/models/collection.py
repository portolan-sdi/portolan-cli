"""CollectionModel dataclass for STAC Collection metadata.

A Collection groups related items with shared extent, license, and schema.
It follows the STAC Collection spec with Portolan extensions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from portolan_cli.models.catalog import Link

# Valid STAC identifier pattern
ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


@dataclass
class Provider:
    """A data provider.

    Attributes:
        name: Provider name.
        roles: Provider roles (licensor, producer, processor, host).
        url: Provider URL.
    """

    name: str
    roles: list[str] | None = None
    url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {"name": self.name}
        if self.roles is not None:
            result["roles"] = self.roles
        if self.url is not None:
            result["url"] = self.url
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Provider:
        """Create Provider from dict."""
        return cls(
            name=data["name"],
            roles=data.get("roles"),
            url=data.get("url"),
        )


@dataclass
class SpatialExtent:
    """Spatial extent with bounding boxes.

    Attributes:
        bbox: List of bounding boxes (WGS84). Each bbox is [west, south, east, north].
    """

    bbox: list[list[float]]

    def __post_init__(self) -> None:
        """Validate bbox values."""
        for box in self.bbox:
            if len(box) not in (4, 6):
                raise ValueError(f"bbox must have 4 or 6 elements, got {len(box)}")

            # Skip antimeridian validation - west > east is valid per STAC
            west, south = box[0], box[1]
            east, north = box[2], box[3]

            # Validate longitude range
            if west < -180 or west > 180 or east < -180 or east > 180:
                raise ValueError(f"longitude must be in [-180, 180], got west={west}, east={east}")

            # Validate latitude range
            if south < -90 or south > 90 or north < -90 or north > 90:
                raise ValueError(f"latitude must be in [-90, 90], got south={south}, north={north}")

            # Validate south <= north (latitude ordering must be correct)
            if south > north:
                raise ValueError(f"south must be <= north, got south={south}, north={north}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {"bbox": self.bbox}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SpatialExtent:
        """Create SpatialExtent from dict."""
        return cls(bbox=data["bbox"])


@dataclass
class TemporalExtent:
    """Temporal extent with intervals.

    Attributes:
        interval: List of temporal intervals. Each is [start, end] with ISO dates or null.
    """

    interval: list[list[str | None]]

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {"interval": self.interval}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TemporalExtent:
        """Create TemporalExtent from dict."""
        return cls(interval=data["interval"])


@dataclass
class ExtentModel:
    """Spatial and temporal extent.

    Attributes:
        spatial: Spatial extent with bboxes.
        temporal: Temporal extent with intervals.
    """

    spatial: SpatialExtent
    temporal: TemporalExtent

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "spatial": self.spatial.to_dict(),
            "temporal": self.temporal.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExtentModel:
        """Create ExtentModel from dict."""
        return cls(
            spatial=SpatialExtent.from_dict(data["spatial"]),
            temporal=TemporalExtent.from_dict(data["temporal"]),
        )


@dataclass
class CollectionModel:
    """STAC Collection metadata model.

    A Collection groups related items with shared extent, license, and schema.

    Attributes:
        id: Unique identifier.
        description: Collection description (required by STAC).
        extent: Spatial and temporal extent.
        type: Always "Collection".
        stac_version: STAC spec version ("1.0.0").
        license: SPDX license identifier (default CC-BY-4.0).
        title: Human-readable title (optional).
        summaries: Aggregated metadata (CRS, geometry types).
        providers: Data providers.
        keywords: Search keywords.
        created: Creation timestamp.
        updated: Last update timestamp.
        links: STAC links.
    """

    id: str
    description: str
    extent: ExtentModel
    type: str = field(default="Collection", init=False)
    stac_version: str = field(default="1.0.0", init=False)
    license: str = "CC-BY-4.0"
    title: str | None = None
    summaries: dict[str, Any] | None = None
    providers: list[Provider] | None = None
    keywords: list[str] | None = None
    created: datetime | None = None
    updated: datetime | None = None
    links: list[Link] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate fields after initialization."""
        if not ID_PATTERN.match(self.id):
            raise ValueError(
                f"Invalid collection id '{self.id}': must match pattern ^[a-zA-Z0-9_-]+$"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "type": self.type,
            "stac_version": self.stac_version,
            "id": self.id,
            "description": self.description,
            "license": self.license,
            "extent": self.extent.to_dict(),
            "links": [link.to_dict() for link in self.links],
        }
        if self.title is not None:
            result["title"] = self.title
        if self.summaries is not None:
            result["summaries"] = self.summaries
        if self.providers is not None:
            result["providers"] = [p.to_dict() for p in self.providers]
        if self.keywords is not None:
            result["keywords"] = self.keywords
        if self.created is not None:
            result["created"] = self.created.isoformat()
        if self.updated is not None:
            result["updated"] = self.updated.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CollectionModel:
        """Create CollectionModel from dict."""
        created = None
        if data.get("created"):
            created = datetime.fromisoformat(data["created"])

        updated = None
        if data.get("updated"):
            updated = datetime.fromisoformat(data["updated"])

        providers = None
        if data.get("providers"):
            providers = [Provider.from_dict(p) for p in data["providers"]]

        links = [Link.from_dict(link) for link in data.get("links", [])]

        collection = cls(
            id=data["id"],
            description=data["description"],
            extent=ExtentModel.from_dict(data["extent"]),
            license=data.get("license", "CC-BY-4.0"),
            title=data.get("title"),
            summaries=data.get("summaries"),
            providers=providers,
            keywords=data.get("keywords"),
            created=created,
            updated=updated,
            links=links,
        )

        # Respect type and stac_version from input if valid (consistency with CatalogModel)
        if "type" in data and data["type"] == "Collection":
            object.__setattr__(collection, "type", data["type"])
        if "stac_version" in data:
            object.__setattr__(collection, "stac_version", data["stac_version"])

        return collection
