# Turn Philadelphia Housing Data Into a Shareable Catalog

Imagine that you maintain useful city data in ArcGIS.
People can view each layer, but they still need to find it, understand it, and combine it.
You want to make that work easier without deploying another server or API.

In this tutorial, you follow the same path as a new Portolan publisher:

1. Convert two public Philadelphia ArcGIS layers.
2. Organize and document them as one catalog.
3. Publish the catalog to object storage.
4. Use the published files to answer a housing question.

You will use Affordable Housing Production and City Council Districts.
The pinned example contains 501 housing projects and all ten districts.

For a larger example, explore the [published Philadelphia housing catalog](https://source.coop/nlebovits/phl-housing-demo).
It contains ten collections and about 1.78 million features.

## Before You Begin

Clone this repository.
Then install Portolan and the tutorial dependencies:

```sh
uv sync --all-extras
```

Run the commands below from the repository root.
The default workflow reads public Philadelphia ArcGIS services.
The City of Philadelphia's [metadata terms](https://metadata.phila.gov/#help/help-faqs/what-are-the-terms-of-use/) apply.

Choose an empty directory for your local catalog:

```sh
export CATALOG_DIR=/tmp/philadelphia-housing
```

## 1. Convert the Source Layers

The tutorial runs this complete script.
Read the commands before you run them.

File: `examples/philadelphia-housing/01-create-catalog.sh`

```sh
#!/bin/sh
# Create a local Portolan catalog from two Philadelphia ArcGIS services.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to a new catalog directory}"

arcgis_url=${PORTOLAN_PHL_ARCGIS_URL:-https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services}

portolan extract arcgis "$arcgis_url" "$CATALOG_DIR" \
    --id philadelphia-housing \
    --services "AffordableHousingProduction,Council_Districts_2024" \
    --workers 2 \
    --retries 3 \
    --license other \
    --license-url "https://metadata.phila.gov/#help/help-faqs/what-are-the-terms-of-use/" \
    --auto
```

Run the script to create the local catalog:

```sh
uv run ./examples/philadelphia-housing/01-create-catalog.sh
```

Portolan searches Philadelphia's ArcGIS services and selects the two named services.
It downloads every page, retries temporary failures, and converts each layer to GeoParquet.
It also preserves the map style from each source layer.

The `--id` flag names the catalog. Without it, Portolan derives the id from
the output directory name. That id is the catalog's public STAC identity, so
a scratch directory makes a poor one.

The output shows this work as it happens:

```text
→ [1/2] Affordable_Housing
Fetching features 1-100 of 501...
Fetching features 101-200 of 501...
HTTP 503 (attempt 1/3)
...
Converted 501 features
Found 25 empty/null geometries.
    ✓ Success
→ [2/2] Council_Districts_2024
Converted 10 features
    ✓ Success
✓ Extracted 2/2 layers
```

The repeatable test service injects the temporary error.
The retry shows that one failed request does not restart the extraction.
Portolan keeps the 25 projects without locations instead of dropping them.
You will see how that affects the final answer.

You now have two collections:

```text
philadelphia-housing/
├── affordablehousingproduction/
└── council_districts_2024/
```

A collection groups the files and information for one subject.
The catalog provides one entry point for both collections.

Portolan currently has direct extractors for ArcGIS REST, WFS, and the CARTO SQL API.
You can also start with local files that Portolan supports.

Directory structure can organize a larger catalog.
Portolan turns intermediate directories into subcatalogs and leaf directories into collections.
Folders below an ArcGIS services root use the same nested structure.

## 2. Add Context and Check the Catalog

Usable data needs more than a file name.
A reader needs a clear description, source, publisher, license, preview, and usage notes.

This tutorial includes prepared metadata for the catalog and two collections.
The script copies these three files into the catalog.

File: `examples/philadelphia-housing/metadata/catalog.yaml`

```yaml
title: "Philadelphia housing"
description: "Affordable housing production and City Council districts for Philadelphia."
contact:
  name: "Portolan Documentation"
  email: "publisher@example.org"
license: "other"
license_url: "https://metadata.phila.gov/#help/help-faqs/what-are-the-terms-of-use/"
providers:
  - name: "City of Philadelphia"
    roles: ["producer", "licensor"]
    url: "https://opendataphilly.org/"
  - name: "Portolan Documentation"
    roles: ["processor", "host"]
    url: "https://portolan-sdi.github.io/portolan-cli/"
source_url: "https://opendataphilly.org/"
processing_notes: "Extracted from two public ArcGIS FeatureServer layers with the Portolan CLI."
```

File: `examples/philadelphia-housing/metadata/affordable_housing.yaml`

```yaml
title: "Affordable Housing Production"
description: "Affordable housing projects funded by Philadelphia's Division of Housing and Community Development and completed since 1994."
contact:
  name: "Portolan Documentation"
  email: "publisher@example.org"
license: "other"
license_url: "https://metadata.phila.gov/#help/help-faqs/what-are-the-terms-of-use/"
providers:
  - name: "City of Philadelphia"
    roles: ["producer", "licensor"]
    url: "https://opendataphilly.org/"
  - name: "Portolan Documentation"
    roles: ["processor", "host"]
    url: "https://portolan-sdi.github.io/portolan-cli/"
source_url: "https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services/AffordableHousingProduction/FeatureServer/0"
processing_notes: "Extracted from ArcGIS, sorted by Hilbert order, and published as GeoParquet."
```

File: `examples/philadelphia-housing/metadata/council_districts_2024.yaml`

```yaml
title: "City Council Districts (2024)"
description: "The ten Philadelphia City Council districts redrawn after the 2020 census."
contact:
  name: "Portolan Documentation"
  email: "publisher@example.org"
license: "other"
license_url: "https://metadata.phila.gov/#help/help-faqs/what-are-the-terms-of-use/"
providers:
  - name: "City of Philadelphia"
    roles: ["producer", "licensor"]
    url: "https://opendataphilly.org/"
  - name: "Portolan Documentation"
    roles: ["processor", "host"]
    url: "https://portolan-sdi.github.io/portolan-cli/"
source_url: "https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services/Council_Districts_2024/FeatureServer/0"
processing_notes: "Extracted from ArcGIS, sorted by Hilbert order, and published as GeoParquet."
```

Replace the placeholder contact before you publish your own catalog.
Portolan accepts an SPDX identifier, such as `CC-BY-4.0`.
Use `other` with a `license_url` when the terms have no SPDX identifier.
This example uses `other` and links to Philadelphia's terms.

The complete second step copies the files, registers the assets, generates docs, and checks the catalog.

File: `examples/philadelphia-housing/02-add-context.sh`

```sh
#!/bin/sh
# Add useful descriptions, previews, and documentation to the local catalog.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to the catalog directory}"

example_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

cp "$example_dir/metadata/catalog.yaml" "$CATALOG_DIR/.portolan/metadata.yaml"
mkdir -p "$CATALOG_DIR/affordablehousingproduction/.portolan"
mkdir -p "$CATALOG_DIR/council_districts_2024/.portolan"
cp \
    "$example_dir/metadata/affordable_housing.yaml" \
    "$CATALOG_DIR/affordablehousingproduction/.portolan/metadata.yaml"
cp \
    "$example_dir/metadata/council_districts_2024.yaml" \
    "$CATALOG_DIR/council_districts_2024/.portolan/metadata.yaml"

portolan add \
    --portolan-dir "$CATALOG_DIR" \
    --force \
    --thumbnails \
    "$CATALOG_DIR/affordablehousingproduction" \
    "$CATALOG_DIR/council_districts_2024"

(cd "$CATALOG_DIR" && portolan readme)
# Refresh the generated documentation asset metadata.
portolan check "$CATALOG_DIR" --fix --workers 1
portolan check "$CATALOG_DIR" --strict

echo "catalog passes the Portolan check."
```

Run the script to apply the metadata and check the result:

```sh
uv run ./examples/philadelphia-housing/02-add-context.sh
```

The important output looks like this:

```text
✓ Added 2 files to 2 collections
✓ Generated 3 README(s)
✓ Created/updated 2 metadata items
✓ Catalog conforms (3 file(s) checked)
catalog passes the Portolan check.
```

This step adds:

- Descriptions and source links.
- Publisher and license information.
- Thumbnails and map styles.
- A `README.md` for each collection.
- An `AGENTS.md` guide for agent users.

Portolan also checks the complete catalog against the Portolan specification.
The full report can include recommendations, but the command stops if a strict requirement fails.
The three generated README files document the catalog and its two collections.

For your own catalog, edit the generated `.portolan/metadata.yaml` files.
This example copies prepared files so that everyone gets the same result.

`.portolan/metadata.yaml` controls descriptions, licenses, providers, and source information.
`.portolan/config.yaml` controls non-sensitive CLI behavior.
Both files can exist at the catalog, subcatalog, or collection level.
Values inherit down the directory tree, and the nearest file overrides its parents.

## 3. Publish the Catalog

When the local catalog looks right, choose an object-storage destination:

```sh
export PORTOLAN_PHL_REMOTE=s3://my-bucket/philadelphia-housing
```

Create the bucket with your storage provider before you publish.
Use the credentials required by your storage provider.
For S3-compatible storage, set `PORTOLAN_S3_ENDPOINT` and `PORTOLAN_S3_USE_SSL`.
Do not store credentials in `.portolan/config.yaml`.
Portolan accepts plaintext HTTP only for an endpoint that uses anonymous access.
It rejects credentials unless the endpoint uses HTTPS.

The publishing step contains one Portolan command.

File: `examples/philadelphia-housing/03-publish.sh`

```sh
#!/bin/sh
# Publish the finished catalog to object storage.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to the catalog directory}"
: "${PORTOLAN_PHL_REMOTE:?Set PORTOLAN_PHL_REMOTE to an object-storage URL}"

portolan push "$PORTOLAN_PHL_REMOTE" --catalog "$CATALOG_DIR"
echo "Published catalog to $PORTOLAN_PHL_REMOTE."
```

Run the script to publish the catalog:

```sh
uv run ./examples/philadelphia-housing/03-publish.sh
```

The output confirms what Portolan uploaded:

```text
→ Found 2 collection(s) to push
✓ [1/2] affordablehousingproduction: 2 version(s), 7 file(s)
✓ [2/2] council_districts_2024: 2 version(s), 7 file(s)
✓ Pushed 2 collection(s), 4 version(s), 18 file(s)
Published catalog to s3://my-bucket/philadelphia-housing.
```

The result is a set of static files in your bucket.
You do not need to deploy a Portolan server, database, or API.
The uploaded files include the GeoParquet assets, metadata, previews, documentation, and version history.

Make the files public according to your storage provider's instructions.
Your catalog now has one HTTP URL that you can share.
That public URL can differ from the storage URL in the publish command.

## 4. Use the Published Catalog

Now ask a practical planning question:

> How many affordable housing projects and units are in each City Council district?

The analysis uses this complete Python program.

File: `examples/philadelphia-housing/query.py`

```python
"""Summarize affordable housing production by City Council district."""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

import duckdb  # deptry: ignore[DEP003] - tutorial analysis, not CLI runtime code


def _asset(catalog: str, collection_id: str) -> str:
    """Return a local path or URL for one GeoParquet asset."""
    parsed = urlparse(catalog)
    relative = f"{collection_id}/{collection_id}.parquet"
    if parsed.scheme in {"http", "https"}:
        return f"{catalog.rstrip('/')}/{relative}"
    return str(Path(catalog) / relative)


def summarize(catalog: str) -> tuple[list[tuple[int, int, int]], tuple[int, int]]:
    """Return district totals and totals for projects without geometry."""
    affordable = _asset(catalog, "affordablehousingproduction")
    districts = _asset(catalog, "council_districts_2024")

    connection = duckdb.connect()
    try:
        connection.execute("INSTALL spatial")
        connection.execute("LOAD spatial")
        rows = connection.execute(
            """
            SELECT
                c.district_num AS district,
                count(*) AS projects,
                sum(a.total_units) AS units
            FROM read_parquet(?) AS c
            JOIN read_parquet(?) AS a
              ON ST_Intersects(c.geometry, a.geometry)
            GROUP BY c.district_num
            ORDER BY c.district_num
            """,
            [districts, affordable],
        ).fetchall()
        missing = connection.execute(
            """
            SELECT count(*), coalesce(sum(total_units), 0)
            FROM read_parquet(?)
            WHERE geometry IS NULL
            """,
            [affordable],
        ).fetchone()
    finally:
        connection.close()

    if missing is None:
        raise RuntimeError("DuckDB did not return the missing-geometry summary")
    return [(int(row[0]), int(row[1]), int(row[2])) for row in rows], (
        int(missing[0]),
        int(missing[1]),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize Philadelphia affordable housing by City Council district."
    )
    parser.add_argument("catalog", help="Local catalog path or public catalog URL")
    return parser.parse_args()


def main() -> None:
    """Run the spatial join and print stable, human-readable results."""
    args = _parse_args()
    rows, missing = summarize(args.catalog)

    print(f"{'district':>8}  {'projects':>8}  {'units':>5}")
    for district, projects, units in rows:
        print(f"{district:>8}  {projects:>8}  {units:>5}")

    print()
    print(f"Located projects: {sum(row[1] for row in rows):,}")
    print(f"Located units: {sum(row[2] for row in rows):,}")
    print(f"Projects without geometry: {missing[0]:,}")
    print(f"Units without geometry: {missing[1]:,}")


if __name__ == "__main__":
    main()
```

Pass the public catalog URL to the program:

```sh
uv run python examples/philadelphia-housing/query.py \
  https://data.example.org/philadelphia-housing
```

The pinned August 27, 2026 snapshot returns:

```text
district  projects  units
       1        37   2310
       2        62   1886
       3        97   3724
       4        36   1172
       5       119   4774
       6         9    264
       7        44   1915
       8        53   1796
       9         9    310
      10        10    433

Located projects: 476
Located units: 18,584
Projects without geometry: 25
Units without geometry: 665
```

District 5 has the most projects and units in this snapshot.
District 3 has the next highest totals.

The result also exposes an important limitation.
Twenty-five projects have no location, so the spatial join cannot assign their 665 units.
A useful analysis reports that gap instead of hiding it.

The analysis uses DuckDB, not a Portolan-specific query service.
DuckDB requests only the required Parquet ranges from object storage.
It installs the spatial extension when the local cache is empty, then loads it.
It does not clone the catalog or download every file first.

You can also use the published files in tools such as QGIS, ArcGIS, or Python.
You can give the same catalog URL to an agent.
The files remain in storage that you control.

Philadelphia can update the live ArcGIS layers.
Your live result can differ from the pinned result above.

## What Portolan Changed

You started with two separate ArcGIS services.
You finished with one documented catalog that people and agents can find, understand, and use.

Portolan handled four parts of the journey:

1. **Convert:** ArcGIS features became cloud-optimized GeoParquet.
2. **Organize:** The files gained consistent structure, metadata, previews, and documentation.
3. **Publish:** The catalog moved to object storage without a new serving layer.
4. **Use:** Standard tools queried the published files directly.

## Use This Pattern With Your Own Data

The same journey works for your ArcGIS services:

1. Change the services root and service names in `01-create-catalog.sh`.
2. Replace the prepared metadata with descriptions and terms for your collections.
3. Publish to storage that you control.
4. Share the catalog URL or use it in your existing tools.

You can also start from files or another direct extraction source named above.
The result has the same basic shape: cloud-native files, clear metadata, and one catalog URL.

## Run the Complete Workflow

The numbered steps are the recommended way to follow this tutorial.
This wrapper contains no hidden work.
It runs the visible steps in the same order.

File: `examples/philadelphia-housing/run.sh`

```sh
#!/bin/sh
# Run the complete Philadelphia housing journey for CI or repeat use.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to a new catalog directory}"

example_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

"$example_dir/01-create-catalog.sh"
"$example_dir/02-add-context.sh"

if [ -n "${PORTOLAN_PHL_REMOTE:-}" ]; then
    "$example_dir/03-publish.sh"
fi
```

Run the wrapper for repeat runs:

```sh
export CATALOG_DIR=/tmp/philadelphia-housing
export PORTOLAN_PHL_REMOTE=s3://my-bucket/philadelphia-housing
uv run ./examples/philadelphia-housing/run.sh
```

## How CI Keeps This Tutorial Working

CI runs the same three scripts against a pinned replay of the Philadelphia services.
It verifies pagination, retry behavior, metadata, previews, documentation, and validation.

CI then publishes the catalog to temporary object storage and stops the ArcGIS replay.
The final test runs the district analysis over HTTP and checks every reported total.

The replay and temporary storage exist only to make the tutorial repeatable.
The user journey and the commands stay the same.
