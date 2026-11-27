"""Data models for Portolan metadata.

This module exports all metadata model classes used throughout Portolan.
Models are dataclasses with JSON serialization support.
"""

from __future__ import annotations

from portolan_core.models.catalog import CatalogModel, Link
from portolan_core.models.collection import (
    CollectionModel,
    ExtentModel,
    Provider,
    SpatialExtent,
    TemporalExtent,
)
from portolan_core.models.item import AssetModel, ItemModel
from portolan_core.models.schema import BandSchema, ColumnSchema, SchemaModel

__all__ = [
    "AssetModel",
    "BandSchema",
    # Catalog
    "CatalogModel",
    # Collection
    "CollectionModel",
    "ColumnSchema",
    "ExtentModel",
    # Item
    "ItemModel",
    "Link",
    "Provider",
    # Schema
    "SchemaModel",
    "SpatialExtent",
    "TemporalExtent",
]
