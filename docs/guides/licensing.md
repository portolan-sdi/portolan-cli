# Licensing

Every collection Portolan writes carries a license. The spec requires one, and
`portolan check` reports a collection without one as an error, so `portolan add`
refuses to write the collection instead of publishing a catalog that fails
validation.

Portolan never picks a license for you. A license is a legal fact about the data,
and only the person publishing it knows what it is.

## The two accepted shapes

A license is either an SPDX identifier, or the literal `other` alongside a URL
pointing at the license text:

```yaml
# .portolan/metadata.yaml
license: "CC-BY-4.0"
```

```yaml
# .portolan/metadata.yaml — for a license with no SPDX identifier
license: "other"
license_url: "https://data.example.org/terms"
```

The second form emits a `rel="license"` link on the collection. Without that link,
`license: "other"` says only that the license is unknown, which the spec rejects.

The deprecated value `proprietary` is not accepted. Use `other` with a URL.

## Setting it when you create the catalog

`portolan init` asks for the license and writes it into
`.portolan/metadata.yaml`:

```console
$ portolan init
Catalog title (optional, press Enter to skip): Roads
Catalog description: Municipal road centerlines
License (SPDX identifier, or 'other'): CC-BY-4.0
```

Scripts and agents pass it as a flag. With `--auto` or `--json` there is nobody to
prompt, so the flag is required:

```bash
portolan init --auto --license CC-BY-4.0

portolan init --auto \
  --license other \
  --license-url https://data.example.org/terms
```

## Where the license lives

`metadata.yaml` files merge from the catalog root down, and the closest file wins.
The root license therefore acts as the default for every collection, and a
collection overrides it by setting its own:

```yaml
# roads/.portolan/metadata.yaml — this collection only
license: "ODbL-1.0"
```

A license already written into `collection.json` also counts, so a collection you
licensed by hand is not asked to repeat itself in `metadata.yaml`.

## Referencing and harvesting data you do not own

`portolan add-external` registers remote data in place. It takes the same two
shapes as flags:

```bash
portolan add-external "s3://bucket/roads/*" \
  --collection roads \
  --license ODbL-1.0
```

The `portolan extract` commands harvest a service's own metadata first. When the
service publishes a license URL, extraction seeds `other` plus that link, which
needs no flag. When it publishes none, pass `--license` and extraction stops
before downloading anything rather than after:

```bash
portolan extract wfs https://example.org/wfs out/ \
  --license other \
  --license-url https://example.org/terms
```

## When Portolan stops

```console
$ portolan add roads/roads.parquet
✗ [PRTLN-VAL004] No usable license for collection 'roads': no license is declared
→ Set 'license:' in .portolan/metadata.yaml to an SPDX identifier such as
  CC-BY-4.0, or to 'other' with a 'license_url:' pointing at the license text.
```

Nothing was written. Set the license and run the command again.

Spelling is checked here too, against the same SPDX list `portolan check` uses.
A typo like `cc-by-4.0` is refused before anything is written, and the message
names the official spelling.
