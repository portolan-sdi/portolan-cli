# Conformance & Versioning

Portolan is a **STAC profile**, not a STAC extension. A catalog does not carry a `portolan:` marker
field; it conforms by satisfying the requirements a validator (`portolan check`) enforces, and it
declares the **profile version** it targets via `conformsTo`.

## Declaring conformance

A Portolan Catalog (and, where it differs, a Collection) SHOULD declare the profile version with a
single core conformance URI, following the OGC API / STAC API `conformsTo` idiom:

```json
{
  "type": "Catalog",
  "id": "example",
  "conformsTo": ["https://portolan-sdi.github.io/spec/v0.2.0/core"]
}
```

- The **version lives in the URI path** (`.../v0.2.0/core`). A version bump changes the URI; clients
  match the URIs they understand (capability negotiation) rather than parsing a bare semver.
- This is the **only** Portolan identity/version signal. There is no `portolan:version` field and no
  Portolan `stac_extensions` entry.
- Declaring it is optional-in-principle (a validator can check against known versions) but recommended
  so tooling can select the right ruleset and give clear diagnostics.

`conformsTo` is formally a STAC API landing-page field; Portolan reuses it on static catalogs. If an
implementation objects to that, the fallback is a single `portolan:conforms_to` field — the *only*
field that would then justify a Portolan extension.

## Capabilities are NOT declared here

Optional capabilities (visualization, semantic, iceberg, …) are **derived by inspecting the
artifacts**, not enumerated in `conformsTo`. See [capabilities.md](capabilities.md). Their *contracts*
are versioned lockstep with the profile, in the spec text.

## Three distinct versions — do not conflate

| Version of… | Mechanism | Owner |
|---|---|---|
| The **STAC spec** | `stac_version` (e.g. `"1.1.0"`) | STAC core field |
| The **Portolan profile** | `conformsTo` URI (`.../v0.2.0/core`) | this spec |
| The **data itself** | `versions.json` / Iceberg snapshots / `version`-extension links | the dataset |

None of these requires a `portolan:version` field.
