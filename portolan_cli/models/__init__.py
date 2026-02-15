"""Data models for Portolan metadata.

This module exports all metadata model classes used throughout Portolan.
Models are dataclasses with JSON serialization support.
"""

from __future__ import annotations

from portolan_cli.models.catalog import CatalogModel, Link
from portolan_cli.models.collection import (
    CollectionModel,
    ExtentModel,
    Provider,
    SpatialExtent,
    TemporalExtent,
)
from portolan_cli.models.item import AssetModel, ItemModel
from portolan_cli.models.schema import BandSchema, ColumnSchema, SchemaModel
from portolan_cli.models.version import AssetVersion, SchemaFingerprint, VersionModel

__all__ = [
    # Catalog
    "CatalogModel",
    "Link",
    # Collection
    "CollectionModel",
    "ExtentModel",
    "SpatialExtent",
    "TemporalExtent",
    "Provider",
    # Schema
    "SchemaModel",
    "ColumnSchema",
    "BandSchema",
    # Item
    "ItemModel",
    "AssetModel",
    # Version
    "VersionModel",
    "SchemaFingerprint",
    "AssetVersion",
]
