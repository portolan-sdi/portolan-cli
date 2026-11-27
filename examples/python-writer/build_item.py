"""Build STAC Items from GeoParquet files and write a catalog with them.

The script reads each GeoParquet file in ./wetlands and writes the catalog
into ./wetlands-catalog in the current folder. To try it with the NWI fixture,
which is in EPSG:5070:

    mkdir wetlands
    cp tests/fixtures/realdata/nwi-wetlands.parquet wetlands/dc-2023.parquet
    cp tests/fixtures/realdata/nwi-wetlands.parquet wetlands/dc-2024.parquet
    uv run python examples/python-writer/build_item.py

tests/docs/test_python_writer_guide.py runs this script and validates the
catalog it writes. docs/guides/python-writer.md includes its sections.
"""

from __future__ import annotations

# --8<-- [start:items]
from pathlib import Path

import pystac

from portolan_cli.api import (
    add_projection_extension,
    add_table_extension,
    add_vector_extension,
    create_collection,
    create_item,
    extract_geoparquet_metadata,
    wgs84_bbox,
)
from portolan_core.writer import LocalSink, apply_plan, plan_catalog

items = []
for data in sorted(Path("wetlands").glob("*.parquet")):
    metadata = extract_geoparquet_metadata(data)
    item = create_item(
        item_id=data.stem,
        bbox=wgs84_bbox(metadata, data),  # metadata.bbox is in the file's CRS
        assets={
            "data": pystac.Asset(
                href=f"./{data.name}",
                media_type="application/vnd.apache.parquet",
                roles=["data"],
                title="Wetlands",
                description="Wetland polygons.",
            )
        },
    )
    add_projection_extension(item, metadata)
    add_vector_extension(item, metadata)
    items.append(item)
# --8<-- [end:items]

# --8<-- [start:collection]
# A plain union. It does not handle Items that cross the antimeridian.
west, south, east, north = zip(*(item.bbox for item in items), strict=True)
collection = create_collection(
    collection_id="wetlands",
    description="National Wetlands Inventory polygons for Washington, DC.",
    license="CC0-1.0",
    bbox=[min(west), min(south), max(east), max(north)],
)
add_table_extension(collection, metadata)
collection.providers = [
    pystac.Provider(name="Wetlands Office", roles=["producer", "host"], url="https://example.org")
]
collection.add_asset(
    "thumbnail",
    pystac.Asset(
        href="./thumbnail.png",
        media_type="image/png",
        roles=["thumbnail"],
        title="Thumbnail",
        description="A map of the wetlands.",
    ),
)
# --8<-- [end:collection]

# --8<-- [start:write]
documents = {
    "catalog.json": {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "wetlands-catalog",
        "title": "Wetlands",
        "description": "Wetland polygons from the National Wetlands Inventory.",
        "links": [],
    },
    "wetlands/collection.json": collection.to_dict(include_self_link=False, transform_hrefs=False),
}
for item in items:
    documents[f"wetlands/{item.id}/{item.id}.json"] = item.to_dict(
        include_self_link=False, transform_hrefs=False
    )

documentation = {
    "README.md": "# Wetlands\n\nWetland polygons.\n",
    "AGENTS.md": "# Wetlands\n\nEach collection holds GeoParquet items.\n",
    "wetlands/README.md": (
        "# Wetlands\n\nWetland polygons in Washington, DC.\n\n## License\n\nCC0-1.0.\n\n"
        "## Provenance\n\nThe National Wetlands Inventory.\n"
    ),
    "wetlands/AGENTS.md": "# Wetlands\n\nRead each item's parquet file with DuckDB.\n",
}

output = Path("wetlands-catalog")
apply_plan(plan_catalog(documents, documentation), LocalSink(output))
# --8<-- [end:write]
