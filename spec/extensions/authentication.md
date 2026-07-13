# Gated catalogs: STAC Authentication (`auth:`) in the Portolan profile

- **Extension:** [STAC Authentication](https://github.com/stac-extensions/authentication) (`auth:`) — an existing STAC extension, adopted; Portolan defines **no authentication namespace of its own**.
- **Depends on:** the `tokenExchange` OAuth2 flow, proposed upstream in [stac-extensions/authentication#44](https://github.com/stac-extensions/authentication/pull/44) (mirroring [OAI/OpenAPI-Specification#5428](https://github.com/OAI/OpenAPI-Specification/pull/5428)).
- **Status:** draft until the upstream flow is released — the extension maintainer has proposed
  releasing it as a **new minor version** of the Authentication extension, decoupled from the
  OpenAPI timeline. The example pins the `v1.1.0` schema, against which it already validates
  (unknown flow keys pass as generic flow objects; strict `tokenUrl` enforcement arrives with
  the new version).

## Summary

A **gated** Portolan catalog keeps its STAC discovery metadata public and static while its data
assets live in access-controlled object storage. A consumer reads such a dataset in two steps,
both fully described by STAC Authentication — no server in the data path, no Portolan-specific
fields:

1. **Identity** — an `openIdConnect` scheme declares the publisher's consumer IdP; the client
   signs in (any OIDC flow, e.g. Authorization Code + PKCE) and obtains an identity token.
2. **Credentials** — an `oauth2` scheme with a `tokenExchange` flow
   ([RFC 8693](https://datatracker.ietf.org/doc/html/rfc8693)) declares the credential-vending
   endpoint: the client POSTs the identity token as `subject_token` to the `tokenUrl` and
   receives short-lived, scoped, read-only storage credentials — or `403` if the publisher has
   not granted that identity access. The client then reads the assets **directly from object
   storage**.

Gated assets reference the exchange scheme via `auth:refs` (the scheme whose output actually
reads the asset). One exchanged credential covers all objects the asset tree needs (e.g. a
GeoParquet file, or an Apache Iceberg table's `metadata.json` + manifests + data files).

## Fields used (all from STAC Authentication — nothing new)

| Field | Where | Use in a gated catalog |
| ----- | ----- | ---------------------- |
| `auth:schemes` | Collection (or Catalog) | Declares the identity scheme (`openIdConnect` + `openIdConnectUrl`) and the exchange scheme (`oauth2` + `flows.tokenExchange.tokenUrl`) |
| `auth:refs` | Asset | Marks a gated asset; references the **exchange** scheme |

Publishers who enforce with provider IAM only (consumers bring their own cloud credentials —
the export-only model of [#120](https://github.com/portolan-sdi/portolan-cli/issues/120))
simply omit the exchange scheme, and MAY declare an `s3` scheme as a hint. Presigned-URL
publishers use the extension's existing `signedUrl` scheme type. Enforcement always stays
outside Portolan: this is discovery metadata only.

## Client algorithm

1. Read `collection.json`. An asset carrying `auth:refs` is gated; resolve the referenced
   scheme.
2. If that scheme has a `tokenExchange` flow, obtain a subject token first: satisfy the scheme
   named by the flow's `subjectTokenScheme` field (see *Chaining*, below).
3. `POST` the identity token to the flow's `tokenUrl` per RFC 8693
   (`grant_type=urn:ietf:params:oauth:grant-type:token-exchange`, `subject_token`,
   `subject_token_type`, optionally `resource`). Receive `access_token` +
   `issued_token_type` + `expires_in`, or `403`.
4. Read the asset `href` directly from storage with the returned credentials (range reads
   work; the credential covers the asset's prefix).

### Chaining (`subjectTokenScheme`)

The `tokenExchange` flow's **`subjectTokenScheme`** field (added to
stac-extensions/authentication#44 after maintainer review) names the `auth:schemes` entry whose
token the client presents as the `subject_token`. It makes the two-step order fully
machine-discoverable as a dependency graph the client simply resolves — no hardcoded order:

```text
asset ──auth:refs──▶ exchange scheme ──subjectTokenScheme──▶ identity scheme
```

In this profile: publishers of gated catalogs **SHOULD** provide `subjectTokenScheme`
(`portolan check` requires it — see Validation); clients **MUST** use it when present. When
absent, the fallback is type-based inference: obtain the subject token via an identity-yielding
scheme (e.g. `openIdConnect`) declared in the same `auth:schemes` object — where several are
declared, any issuer the vending endpoint trusts is acceptable (the endpoint validates the
issuer regardless).

## Validation (`portolan check`)

For any Collection/Catalog carrying `auth:schemes`, and any asset carrying `auth:refs`:

- every `auth:refs` entry MUST resolve to a key of an `auth:schemes` object in scope;
- an `openIdConnect` scheme MUST carry `openIdConnectUrl`;
- an `oauth2` scheme MUST carry `flows`, and a `tokenExchange` flow MUST carry `tokenUrl`
  (schema-enforced upstream once #44 is released);
- a `tokenExchange` flow MUST carry `subjectTokenScheme`, and its value MUST resolve to a key
  of the same `auth:schemes` object (cross-reference resolution is not expressible in the
  upstream JSON Schema, so this profile enforces it);
- private (non-anonymously-readable) data assets SHOULD carry `auth:refs`.

## Security considerations

- `auth:schemes` SHOULD point at the publisher's **consumer** IdP, never an operator/editor
  IdP: the population that authenticates to read data is distinct from the one that
  administers the catalog.
- Neither scheme carries secrets; an OIDC public client id is not a secret, and vended
  credentials exist only in exchange responses, never in metadata.
- Vended credentials are expected to be short-lived, read-only, and scoped to the minimum
  asset prefix; revocation is a vending-endpoint concern (short TTLs bound already-issued
  credentials).

## Example

See [`../examples/authentication-collection.json`](../examples/authentication-collection.json):
a gated collection with the two schemes and two gated assets (GeoParquet + Iceberg table
metadata) sharing one exchange.

## History

This PR originally proposed a Portolan-owned `access:` extension carrying the vending pointer
(`access:credentials`). The discussion resolved into standardizing the missing piece upstream
(the `tokenExchange` flow) and adopting STAC Authentication wholesale — the earlier proposal
remains in this PR's history.
