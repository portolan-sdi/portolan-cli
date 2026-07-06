# Access Extension (`access:`)

- **Namespace:** `access:`
- **Scope:** Catalog, Collection
- **Extension URI:** `https://portolan.org/stac-extensions/access/v1.0.0/schema.json`
- **Maturity:** Proposal (per [ADR-0037](../../context/shared/adr/0037-experimental-extension-policy.md): Proposal → Pilot → Stable)
- **Maturity path:** incubating **within Portolan** — rapid iteration and validation against a
  running implementation. Once stable, the intent is to propose it to the **STAC community**
  (stac-extensions) together with a working end-to-end implementation, as a concrete example
  rather than a spec-only proposal.
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
**enforcement** (provider-specific: who *actually* can). Its **recommended
enforcement is export-only provider IAM** — compile a portable policy into
S3/GCS/MinIO IAM and let the storage layer enforce it; consumers are IAM
principals, **no running service** (the "static files, no servers" path).
User authentication and **brokered credential vending** are the enforcement
piece #120 places **beyond the CLI's metadata role**, in a separate managed
layer.

This extension is the **consumer-facing wire contract for that managed layer** —
it lets a client *discover* that a broker exists and how to use it. It does
**not** compete with export-only IAM: a publisher enforcing that way simply
omits this extension. The two are complementary, and the spec stays neutral
about which a publisher picks. (Visibility tagging — public/private, `--visibility` — is deliberately
**out of scope** here: it belongs to the #120 discussion and the publisher's own
tooling, not this read-time contract.)

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
| `access:credentials` | object | **Yes** | How to exchange a verified identity token (obtained via a STAC `auth:` scheme) for short-lived storage credentials. **This is the only field this extension defines** — everything else a private catalog needs (visibility tiers, tenancy, registration UX, analytics) is the publisher's own concern and is intentionally kept out of the open contract. |

### `access:credentials` object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string (URI) | **Yes** | HTTPS endpoint. The client `POST`s the bearer token plus the target collection/asset; it returns short-lived, scoped credentials, or `403` if not entitled. |
| `protocol` | string | **Yes** | Credential wire format: `"portolan-broker/1.0"` (default) or `"opensharing"` (both return short-lived scoped credentials — best for Iceberg/multi-file, one credential reads many objects); or `"presigned"` (per-object presigned URLs — universal across clouds, simplest client). |
| `scheme` | string | No | Key of the `auth:schemes` entry whose token the endpoint accepts (links this exchange to a specific STAC auth scheme). |

### Link relations

Complement `rel: "via"` (data provenance) with access-flow endpoints:

| `rel` | Description |
|-------|-------------|
| `credentials` | The credential-vending endpoint (same URL as `access:credentials.endpoint`). |

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
step 4 returns `401/403`.

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
2. Standardize the `access:credentials.endpoint` request/response body, or defer
   it entirely to the named `protocol` profile?
3. Could the vending step instead become a new scheme `type` (e.g.
   `tokenExchange`) contributed upstream to the STAC Authentication extension,
   removing the need for a separate namespace? **Deferred, not open now:** per
   the maturity path above, the extension incubates within Portolan first; the
   upstream form (this extension as-is, or a scheme `type`) is decided when it
   is stable and proposed to the STAC community with a working end-to-end
   implementation.
4. Schema-hosting domain: extensions reference `portolan.org/stac-extensions/…`
   (ADR-0042) while core schemas use `portolan.dev/schema/…`; to be reconciled.
