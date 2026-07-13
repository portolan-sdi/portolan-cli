"""Directory scanning, classification, inference, and catalog ``check``.

The orchestrator lives in :mod:`portolan_cli.scan.core`; import its public API
as ``from portolan_cli.scan.core import scan_directory`` (etc.). This ``__init__``
is intentionally import-free: importing a leaf submodule (e.g. ``scan.detect``)
must not eagerly pull in the heavy orchestrator, which would reintroduce the
``collection_id`` <-> ``scan`` import cycle (#625).
"""
