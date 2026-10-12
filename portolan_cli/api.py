"""The public API for Portolan plugins (#947).

A plugin imports from this module and from ``portolan_cli.hookspecs``, and
from nothing else in ``portolan_cli``. Both follow semantic versioning. A
change that removes a name or breaks its signature needs a major release.

Each name loads from its module when a plugin reads it, so ``import
portolan_cli.api`` loads no Click and no geospatial library. A plugin can import
it at module level. Every read resolves the source module again, as a
function-level import does, so a test that patches the source module reaches
the plugin.

Groups:

- Hooks: ``hookimpl``, ``EnrichContext``.
- Format handlers: ``FormatHandler``, ``FormatMetadata``.
- Versioning backends: ``VersioningBackend``, ``DriftReport``,
  ``SchemaFingerprint``, ``PostAddContext``, ``PostAddItemContext``,
  ``Version``, ``Asset``, ``SchemaInfo``, ``parse_version``, ``get_backend``.
- Catalogs: ``find_catalog_root``, ``get_setting``, ``write_json_atomic``.
- Remote storage: ``upload_file``, ``download_file``, ``PullResult``.
- Terminal output: ``success``, ``info``, ``warn``, ``error``, ``detail``.
- JSON output for commands: ``success_envelope``, ``error_envelope``,
  ``ErrorDetail``. A command writes ``envelope.to_json()`` to stdout.
- Warnings: ``PluginWarning``.
- Format handler helpers: the converters, metadata readers, describe and
  inspect helpers, thumbnails, validation, and partitioning that the built-in
  format handlers in ``portolan_cli.formats_builtin`` use. A plugin handler
  for a similar format can use them too.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from portolan_cli.lazy_exports import lazy_exports

if TYPE_CHECKING:
    from portolan_cli.backends import get_backend
    from portolan_cli.backends.protocol import (
        DriftReport,
        PostAddContext,
        PostAddItemContext,
        SchemaFingerprint,
        VersioningBackend,
    )
    from portolan_cli.catalog import find_catalog_root
    from portolan_cli.config import get_setting
    from portolan_cli.convert import (
        validate_cog,
        validate_geoparquet,
        write_raster_thumbnail,
    )
    from portolan_cli.errors import InvalidOutputError
    from portolan_cli.finalization import aggregate_table_extension
    from portolan_cli.format_families import RASTER_EXTENSIONS, VECTOR_EXTENSIONS, is_geojson
    from portolan_cli.formats import is_geoparquet
    from portolan_cli.hookspecs import EnrichContext, hookimpl
    from portolan_cli.inspect import FileInfo, inspect_cog, inspect_geoparquet
    from portolan_cli.json_io import write_json_atomic
    from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope
    from portolan_cli.metadata.cog import extract_cog_metadata
    from portolan_cli.metadata.flatgeobuf import extract_flatgeobuf_metadata
    from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
    from portolan_cli.metadata.pmtiles import extract_pmtiles_metadata
    from portolan_cli.output import detail, error, info, success, warn
    from portolan_cli.partitioning import partition_vector_file
    from portolan_cli.plugins.formats import FormatContext, FormatHandler, FormatMetadata
    from portolan_cli.plugins.manager import PluginWarning
    from portolan_cli.preparation import (
        convert_raster,
        convert_vector,
        describe_raster_item,
        describe_vector_item,
    )
    from portolan_cli.sync.download import download_file
    from portolan_cli.sync.pull import PullResult
    from portolan_cli.sync.upload import upload_file
    from portolan_cli.versions import Asset, SchemaInfo, Version, parse_version

__all__ = [
    "Asset",
    "DriftReport",
    "EnrichContext",
    "ErrorDetail",
    "FileInfo",
    "FormatContext",
    "FormatHandler",
    "FormatMetadata",
    "InvalidOutputError",
    "PluginWarning",
    "PostAddContext",
    "PostAddItemContext",
    "PullResult",
    "RASTER_EXTENSIONS",
    "SchemaFingerprint",
    "SchemaInfo",
    "VECTOR_EXTENSIONS",
    "Version",
    "VersioningBackend",
    "aggregate_table_extension",
    "convert_raster",
    "convert_vector",
    "describe_raster_item",
    "describe_vector_item",
    "detail",
    "download_file",
    "error",
    "error_envelope",
    "extract_cog_metadata",
    "extract_flatgeobuf_metadata",
    "extract_geoparquet_metadata",
    "extract_pmtiles_metadata",
    "find_catalog_root",
    "get_backend",
    "get_setting",
    "hookimpl",
    "info",
    "inspect_cog",
    "inspect_geoparquet",
    "is_geojson",
    "is_geoparquet",
    "parse_version",
    "partition_vector_file",
    "success",
    "success_envelope",
    "upload_file",
    "validate_cog",
    "validate_geoparquet",
    "warn",
    "write_json_atomic",
    "write_raster_thumbnail",
]

_EXPORTS = {
    "Asset": "portolan_cli.versions",
    "DriftReport": "portolan_cli.backends.protocol",
    "EnrichContext": "portolan_cli.hookspecs",
    "ErrorDetail": "portolan_cli.json_output",
    "FileInfo": "portolan_cli.inspect",
    "FormatContext": "portolan_cli.plugins.formats",
    "FormatHandler": "portolan_cli.plugins.formats",
    "FormatMetadata": "portolan_cli.plugins.formats",
    "InvalidOutputError": "portolan_cli.errors",
    "PluginWarning": "portolan_cli.plugins.manager",
    "PostAddContext": "portolan_cli.backends.protocol",
    "PostAddItemContext": "portolan_cli.backends.protocol",
    "PullResult": "portolan_cli.sync.pull",
    "RASTER_EXTENSIONS": "portolan_cli.format_families",
    "SchemaFingerprint": "portolan_cli.backends.protocol",
    "SchemaInfo": "portolan_cli.versions",
    "VECTOR_EXTENSIONS": "portolan_cli.format_families",
    "Version": "portolan_cli.versions",
    "VersioningBackend": "portolan_cli.backends.protocol",
    "aggregate_table_extension": "portolan_cli.finalization",
    "convert_raster": "portolan_cli.preparation",
    "convert_vector": "portolan_cli.preparation",
    "describe_raster_item": "portolan_cli.preparation",
    "describe_vector_item": "portolan_cli.preparation",
    "detail": "portolan_cli.output",
    "download_file": "portolan_cli.sync.download",
    "error": "portolan_cli.output",
    "error_envelope": "portolan_cli.json_output",
    "extract_cog_metadata": "portolan_cli.metadata.cog",
    "extract_flatgeobuf_metadata": "portolan_cli.metadata.flatgeobuf",
    "extract_geoparquet_metadata": "portolan_cli.metadata.geoparquet",
    "extract_pmtiles_metadata": "portolan_cli.metadata.pmtiles",
    "find_catalog_root": "portolan_cli.catalog",
    "get_backend": "portolan_cli.backends",
    "get_setting": "portolan_cli.config",
    "hookimpl": "portolan_cli.hookspecs",
    "info": "portolan_cli.output",
    "inspect_cog": "portolan_cli.inspect",
    "inspect_geoparquet": "portolan_cli.inspect",
    "is_geojson": "portolan_cli.format_families",
    "is_geoparquet": "portolan_cli.formats",
    "parse_version": "portolan_cli.versions",
    "partition_vector_file": "portolan_cli.partitioning",
    "success": "portolan_cli.output",
    "success_envelope": "portolan_cli.json_output",
    "upload_file": "portolan_cli.sync.upload",
    "validate_cog": "portolan_cli.convert",
    "validate_geoparquet": "portolan_cli.convert",
    "warn": "portolan_cli.output",
    "write_json_atomic": "portolan_cli.json_io",
    "write_raster_thumbnail": "portolan_cli.convert",
}


__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, __all__, globals(), cache=False)
