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
- Versioning backends: ``VersioningBackend``, ``DriftReport``,
  ``SchemaFingerprint``, ``PostAddContext``, ``PostAddItemContext``,
  ``Version``, ``Asset``, ``SchemaInfo``, ``parse_version``, ``get_backend``.
- Catalogs: ``find_catalog_root``, ``get_setting``, ``href_root``.
- Remote storage: ``upload_file``, ``download_file``, ``PullResult``.
- Terminal output: ``success``, ``info``, ``warn``, ``error``, ``detail``.
- JSON output for commands: ``success_envelope``, ``error_envelope``,
  ``ErrorDetail``. A command writes ``envelope.to_json()`` to stdout.
- Warnings: ``PluginWarning``.
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
    from portolan_cli.hookspecs import EnrichContext, hookimpl
    from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope
    from portolan_cli.output import detail, error, info, success, warn
    from portolan_cli.plugins.manager import PluginWarning
    from portolan_cli.sync.download import download_file
    from portolan_cli.sync.pull import PullResult
    from portolan_cli.sync.upload import upload_file
    from portolan_cli.utils import href_root
    from portolan_cli.versions import Asset, SchemaInfo, Version, parse_version

__all__ = [
    "Asset",
    "DriftReport",
    "EnrichContext",
    "ErrorDetail",
    "PluginWarning",
    "PostAddContext",
    "PostAddItemContext",
    "PullResult",
    "SchemaFingerprint",
    "SchemaInfo",
    "Version",
    "VersioningBackend",
    "detail",
    "download_file",
    "error",
    "error_envelope",
    "find_catalog_root",
    "get_backend",
    "get_setting",
    "hookimpl",
    "href_root",
    "info",
    "parse_version",
    "success",
    "success_envelope",
    "upload_file",
    "warn",
]

_EXPORTS = {
    "Asset": "portolan_cli.versions",
    "DriftReport": "portolan_cli.backends.protocol",
    "EnrichContext": "portolan_cli.hookspecs",
    "ErrorDetail": "portolan_cli.json_output",
    "PluginWarning": "portolan_cli.plugins.manager",
    "PostAddContext": "portolan_cli.backends.protocol",
    "PostAddItemContext": "portolan_cli.backends.protocol",
    "PullResult": "portolan_cli.sync.pull",
    "SchemaFingerprint": "portolan_cli.backends.protocol",
    "SchemaInfo": "portolan_cli.versions",
    "Version": "portolan_cli.versions",
    "VersioningBackend": "portolan_cli.backends.protocol",
    "detail": "portolan_cli.output",
    "download_file": "portolan_cli.sync.download",
    "error": "portolan_cli.output",
    "error_envelope": "portolan_cli.json_output",
    "find_catalog_root": "portolan_cli.catalog",
    "get_backend": "portolan_cli.backends",
    "get_setting": "portolan_cli.config",
    "hookimpl": "portolan_cli.hookspecs",
    "href_root": "portolan_cli.utils",
    "info": "portolan_cli.output",
    "parse_version": "portolan_cli.versions",
    "success": "portolan_cli.output",
    "success_envelope": "portolan_cli.json_output",
    "upload_file": "portolan_cli.sync.upload",
    "warn": "portolan_cli.output",
}


__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, __all__, globals(), cache=False)
