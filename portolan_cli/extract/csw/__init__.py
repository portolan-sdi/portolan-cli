"""CSW (Catalog Service for Web) metadata extraction."""

from __future__ import annotations

from portolan_cli.extract.csw.client import (
    detect_metadata_url_type,
    fetch_metadata_for_layer,
    fetch_metadata_record,
    is_metadata_url_supported,
)
from portolan_cli.extract.csw.iso_parser import ISOParseError, parse_iso19139
from portolan_cli.extract.csw.models import ISOMetadata

__all__ = [
    "ISOMetadata",
    "ISOParseError",
    "detect_metadata_url_type",
    "fetch_metadata_for_layer",
    "fetch_metadata_record",
    "is_metadata_url_supported",
    "parse_iso19139",
]
