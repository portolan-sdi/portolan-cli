"""Schema export/import functionality for Portolan.

This module provides round-trip schema editing capabilities:
- Export schema.json to JSON, CSV, or Parquet for editing
- Import edited schema back with validation
- Detect breaking changes between schema versions
"""

from __future__ import annotations

from portolan_cli.schema.breaking import (
    BreakingChange,
    detect_breaking_changes,
    is_breaking,
)
from portolan_cli.schema.export import (
    export_schema_csv,
    export_schema_json,
    export_schema_parquet,
)
from portolan_cli.schema.import_ import (
    import_schema_csv,
    import_schema_json,
    import_schema_parquet,
)

__all__: list[str] = [
    "export_schema_json",
    "export_schema_csv",
    "export_schema_parquet",
    "import_schema_json",
    "import_schema_csv",
    "import_schema_parquet",
    "BreakingChange",
    "detect_breaking_changes",
    "is_breaking",
]
