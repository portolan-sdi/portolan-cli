"""Write a small Portolan catalog from Python with portolan-core.

Run it with an output folder:

    uv run python examples/python-writer/write_catalog.py ./my-catalog

tests/docs/test_python_writer_guide.py runs this script and validates the
catalog it writes. docs/guides/python-writer.md includes its sections.
"""

from __future__ import annotations

import sys
from pathlib import Path

# --8<-- [start:plan]
from portolan_core.writer import LocalSink, apply_plan, plan_catalog

PROFILE = "https://schemas.portolan-sdi.org/portolan/v0.3.0/schema.json"

documents = {
    "catalog.json": {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PROFILE],
        "id": "city-open-data",
        "title": "City Open Data",
        "description": "Open geospatial data from the city.",
        "links": [],
    },
    "parks/collection.json": {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PROFILE],
        "id": "parks",
        "title": "City Parks",
        "description": "Park boundaries as one GeoParquet file.",
        "license": "CC-BY-4.0",
        "providers": [
            {
                "name": "City Parks Office",
                "roles": ["producer", "host"],
                "url": "https://example.org",
            }
        ],
        "extent": {
            "spatial": {"bbox": [[-75.3, 39.8, -74.9, 40.1]]},
            "temporal": {"interval": [["2025-01-01T00:00:00Z", None]]},
        },
        "assets": {
            "parks": {
                "href": "./parks.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
                "title": "Parks",
                "description": "Park boundary polygons.",
            },
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "roles": ["thumbnail"],
                "title": "Thumbnail",
                "description": "A map of the parks.",
            },
        },
        "links": [],
    },
}

documentation = {
    "README.md": "# City Open Data\n\nOpen data from the city.\n",
    "AGENTS.md": "# City Open Data\n\nEach collection holds one GeoParquet file.\n",
    "parks/README.md": (
        "# City Parks\n\nPark boundaries.\n\n## License\n\nCC-BY-4.0.\n\n"
        "## Provenance\n\nThe City Parks Office.\n"
    ),
    "parks/AGENTS.md": "# City Parks\n\nRead parks.parquet with DuckDB or GeoPandas.\n",
}

plan = plan_catalog(documents, documentation)
# --8<-- [end:plan]


def main(output: Path) -> None:
    """Apply the plan to ``output``, then print the operations."""
    # --8<-- [start:apply]
    apply_plan(plan, LocalSink(output))
    # --8<-- [end:apply]
    # --8<-- [start:loop]
    for operation in plan.operations:
        print(type(operation).__name__, operation.key)
    # --8<-- [end:loop]


if __name__ == "__main__":
    main(Path(sys.argv[1]))
