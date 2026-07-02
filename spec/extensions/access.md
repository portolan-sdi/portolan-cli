# Access Extension (`access:`)

- **Namespace:** `access:`
- **Scope:** Catalog, Collection
- **Extension URI:** `https://portolan.org/stac-extensions/access/v1.0.0/schema.json`
- **Maturity:** Proposal (per [ADR-0037](../../context/shared/adr/0037-experimental-extension-policy.md): Proposal → Pilot → Stable)
- **Builds on:** [STAC Authentication extension](https://github.com/stac-extensions/authentication) (`auth:`)

## Summary

A small, **open, vendor-neutral** companion to the STAC **Authentication**
extension. It adds the one thing that extension does not model: a
**credential-vending endpoint** — where a client exchanges a verified identity
token for **short-lived, scoped storage credentials** to read otherwise-private
data bytes directly from object storage.

The catalog's STAC **metadata stays public and discoverable**; only the **data
bytes** are gated. A consumer authenticates with a standard STAC `auth:` scheme,
then calls the `access:credentials` endpoint to obtain temporary read
credentials.

> **Don't reinvent identity.** Declaring *how to authenticate* (OIDC/OAuth2
> issuer, flows, scopes) is already standardized by the STAC Authentication
> extension via `auth:schemes` / `auth:refs`. This extension reuses it and only
> adds the *credential-exchange* step. Per ADR-0037 it uses a dedicated
> `access:` namespace (not a `portolan:` prefix).

## Relationship to access-control models (issue #120)

Portolan issue [#120 (Access Control & Visibility)](https://github.com/portolan-sdi/portolan-cli/issues/120)
frames access control as **policy** (portable metadata: who *should* access) vs
**enforcement** (provider-specific: who *actually* can). It contemplates two
enforcement paths:

1. **Export-only / IAM** — compile a portable policy into provider IAM
   (S3/GCS/MinIO) and let the storage layer enforce it. Consumers are IAM
   principals; **no running service**. (The "static files, no servers" path.)
2. **Brokered credential vending** — a control-plane service verifies an
   external identity and vends short-lived scoped credentials at read time.

This extension is the **consumer-facing wire contract for path 2** — it lets a
client *discover* that a broker exists and how to use it. It does **not** mandate
path 2; a publisher using export-only IAM simply omits this extension. The two
paths are complementary, and the spec stays neutral about which a publisher
picks. The `access:tier` field aligns with #120's proposed `--visibility`
metadata tag.

## Design principles

- **Metadata open, data gated.** Discovery (catalog.json / STAC / Records) is
  anonymous; only the bytes require credentials. Follows naturally from
  `SELF_CONTAINED` (relative links) + absolute-asset-`href`.
- **Compose, don't duplicate.** Identity via STAC `auth:`; this extension adds
  only credential vending.
- **Open contract, any implementation.** Standard protocols (OAuth2 / OIDC for
  identity; an STS-style vending endpoint for storage creds). Any broker that
  honors it interoperates; no proprietary wire protocol, no lock-in.
- **Additive / backward-compatible.** A client that doesn't understand the
  extension still reads valid STAC. Public datasets omit it.
- **Least privilege.** Vended credentials are expected to be read-only, scoped
  to the asset prefix, and short-lived.

## Fields

All fields use the `access:` namespace and MAY appear on a Collection (most
common) or a Catalog (catalog-wide default). (The extension version is carried
by the schema URI in `stac_extensions`, not by a field.)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `access:tier` | string | No | Publisher visibility intent: `"listed"` (metadata public, data gated) or `"private"` (metadata also gated). Default `"listed"`. Informational; enforcement is out of band. Aligns with #120 `--visibility`. |
| `access:credentials` | object | **Yes** | How to exchange a verified identity token (obtained via a STAC `auth:` scheme) for short-lived storage credentials. |
| `access:register` | string (URI) | No | Where a new consumer requests or self-registers for access (human-facing). |

### `access:credentials` object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string (URI) | **Yes** | HTTPS endpoint. The client `POST`s the bearer token plus the target collection/asset; it returns short-lived, scoped credentials, or `403` if not entitled. |
| `protocol` | string | **Yes** | Credential wire format: `"portolan-broker/1.0"` (default) or `"opensharing"` (both return short-lived scoped credentials — best for Iceberg/multi-file, one credential reads many objects); or `"presigned"` (per-object presigned URLs — universal across clouds, simplest client). |
| `scheme` | string | No | Key of the `auth:schemes` entry whose token the endpoint accepts (links this exchange to a specific STAC auth scheme). |
| `audience` | string | No | Token audience the endpoint expects, when the IdP requires it. |

### Link relations

Complement `rel: "via"` (data provenance) with access-flow endpoints:

| `rel` | Description |
|-------|-------------|
| `credentials` | The credential-vending endpoint (same URL as `access:credentials.endpoint`). |
| `register` | Registration / request-access page (same URL as `access:register`). |

(The IdP / login endpoint is already described by the STAC `auth:` scheme; no
extra link relation is needed for it.)

## Client algorithm

1. Read `collection.json`. If a `data` asset has `auth:refs` and the collection
   carries `access:credentials`, the bytes are gated via a broker.
2. Resolve the referenced `auth:schemes` entry and authenticate (e.g.
   `openIdConnect` → OIDC discovery → Authorization Code + PKCE). Obtain a token.
3. `POST` the token to `access:credentials.endpoint` with the target collection
   (and optionally asset). Receive short-lived, prefix-scoped, read-only storage
   credentials — or `403` if no entitlement exists.
4. Use those credentials to range-read the absolute asset `href`.

A client that implements neither extension still has valid STAC; the read in
step 4 returns `401/403`, and the client MAY surface `access:register`.

## `stac_extensions`

A gated Collection lists **both** the STAC Authentication extension (for
identity) and this extension (for vending):

```json
"stac_extensions": [
  "https://stac-extensions.github.io/authentication/v1.1.0/schema.json",
  "https://portolan.org/stac-extensions/access/v1.0.0/schema.json"
]
```

## Security considerations

- Neither extension carries secrets. OAuth/OIDC `client_id` (in `auth:schemes`)
  is a public identifier; there is no client secret in metadata.
- Credentials returned by `access:credentials.endpoint` MUST be short-lived and
  SHOULD be read-only and scoped to the minimum asset prefix.
- Revocation is a control-plane concern of the broker; already-issued
  credentials remain valid until expiry, so TTLs SHOULD be short.
- The referenced `auth:schemes` entry SHOULD point at the **consumer** identity
  provider, never an operator/editor IdP: the population that authenticates to
  *read data* is distinct from the one that *administers* the catalog, and
  conflating them lets a consumer credential reach the control plane.

## Relationship to OpenSharing

`access:credentials.protocol: "opensharing"` lets a publisher reuse the
OpenSharing (Delta Sharing successor) credential wire format — bearer token in →
scoped temporary storage credentials out — so consumers in that ecosystem can
read a Portolan private collection without bespoke client code.

## Example

See [`../examples/access-collection.json`](../examples/access-collection.json).

## Open questions

1. Per-asset `access:` overrides vs. collection-level only (note `auth:refs` is
   already per-asset).
2. Whether `access:tier: "private"` (gated metadata) needs a companion
   authenticated discovery index, or is purely a publisher hint.
3. Standardize the `access:credentials.endpoint` request/response body, or defer
   it entirely to the named `protocol` profile?
4. Could the vending step instead become a new scheme `type` (e.g.
   `tokenExchange`) contributed upstream to the STAC Authentication extension,
   removing the need for a separate namespace? (Tracked against #120.)
5. Schema-hosting domain: extensions reference `portolan.org/stac-extensions/…`
   (ADR-0042) while core schemas use `portolan.dev/schema/…`; to be reconciled.
