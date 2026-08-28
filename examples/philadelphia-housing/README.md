# Turn Philadelphia housing data into a shareable catalog

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

## Before you begin

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

## 1. Convert the source layers

Create the local catalog:

```sh
uv run ./examples/philadelphia-housing/01-create-catalog.sh
```

Portolan searches Philadelphia's ArcGIS services and selects the two named services.
It downloads every page, retries temporary failures, and converts each layer to GeoParquet.
It also preserves the map style from each source layer.

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

## 2. Add context and check the catalog

Usable data needs more than a file name.
A reader needs a clear description, source, publisher, license, preview, and usage notes.

This tutorial includes prepared metadata for the two Philadelphia collections.
Apply it, generate the documentation, and check the result:

Portolan accepts an SPDX identifier, such as `CC-BY-4.0`.
Use `other` with a `license_url` when the terms have no SPDX identifier.
This example uses `other` and links to Philadelphia's terms.

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

## 3. Publish the catalog

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

Publish the catalog:

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

## 4. Use the published catalog

Now ask a practical planning question:

> How many affordable housing projects and units are in each City Council district?

Pass the public catalog URL to the included analysis:

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

## What Portolan changed

You started with two separate ArcGIS services.
You finished with one documented catalog that people and agents can find, understand, and use.

Portolan handled four parts of the journey:

1. **Convert:** ArcGIS features became cloud-optimized GeoParquet.
2. **Organize:** The files gained consistent structure, metadata, previews, and documentation.
3. **Publish:** The catalog moved to object storage without a new serving layer.
4. **Use:** Standard tools queried the published files directly.

## Use this pattern with your own data

The same journey works for your ArcGIS services:

1. Change the services root and service names in `01-create-catalog.sh`.
2. Replace the prepared metadata with descriptions and terms for your collections.
3. Publish to storage that you control.
4. Share the catalog URL or use it in your existing tools.

You can also start from files or another direct extraction source named above.
The result has the same basic shape: cloud-native files, clear metadata, and one catalog URL.

## Run the complete workflow

The numbered steps are the recommended way to follow this tutorial.
For repeat runs, the wrapper executes them in the same order:

```sh
export CATALOG_DIR=/tmp/philadelphia-housing
export PORTOLAN_PHL_REMOTE=s3://my-bucket/philadelphia-housing
uv run ./examples/philadelphia-housing/run.sh
```

## How CI keeps this tutorial working

CI runs the same three scripts against a pinned replay of the Philadelphia services.
It verifies pagination, retry behavior, metadata, previews, documentation, and validation.

CI then publishes the catalog to temporary object storage and stops the ArcGIS replay.
The final test runs the district analysis over HTTP and checks every reported total.

The replay and temporary storage exist only to make the tutorial repeatable.
The user journey and the commands stay the same.
