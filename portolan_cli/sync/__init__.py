"""Remote sync: push, pull, upload, download, checksums, and clone/sync orchestration.

The orchestrator lives in :mod:`portolan_cli.sync.core`; import its public API
as ``from portolan_cli.sync.core import sync`` (etc.). This ``__init__`` is
intentionally import-free so importing a leaf submodule (e.g. ``sync.checksums``)
does not eagerly run the orchestrator, which would reintroduce the ``add`` <->
``sync`` import cycle (#625).
"""
