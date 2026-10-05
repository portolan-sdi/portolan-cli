# MinIO withdrew its community images, so the e2e stack cannot pull them

**Status:** open. `Documentation Build` and `Iceberg E2E Tests` fail on every pull request.
**Tracking issue:** [#919](https://github.com/portolan-sdi/portolan-cli/issues/919). An earlier round is [#874](https://github.com/portolan-sdi/portolan-cli/issues/874), closed on [#877](https://github.com/portolan-sdi/portolan-cli/pull/877).
**Affected files:** `tests/iceberg/e2e/docker-compose.yml`, and the `docs` job in `.github/workflows/ci.yml` that starts the same file.
**Found by:** the Iceberg alignment work, when every branch failed the same two jobs.

## What happens

Both jobs stop when Docker pulls the MinIO image. No test in either job runs.

```
 minio Pulling
 minio Error unauthorized: access to the requested resource is not authorized
Error response from daemon: unauthorized: access to the requested resource is not authorized
```

The failure belongs to no branch. `Documentation Build` has no path filter, so it
runs on every pull request, including pull requests from people outside the team.

## The distribution, not the registry

MinIO withdrew its community distribution. No channel below serves the images:

| Channel | Result |
|---|---|
| `minio/minio`, `minio/mc`, `minio/console` on Docker Hub and quay.io | 401 |
| `dl.min.io` server and client binaries | 410 Gone |
| `ghcr.io/minio/minio` | 403 |
| GitHub `minio/minio` and `minio/mc` | archived |

#877 moved the images from Docker Hub to quay.io and #874 closed on it. quay.io
refuses them too.

Authentication does not help. Docker Hub grants an anonymous pull token and that
token grants no access at all, which marks a private repository rather than a
throttled one:

```
$ curl -s "https://auth.docker.io/token?service=registry.docker.io&scope=repository:minio/minio:pull" \
  | jq -r .token | cut -d. -f2 | base64 -d | jq .access
[]

# the same request for a public image
$ ... repository:library/alpine:pull ...
[{"type":"repository","name":"library/alpine","actions":["pull"], ...}]
```

So only an account MinIO has entitled can pull. Logging in with an ordinary
account changes nothing.

## Requirements on an S3 server

More than the S3 API. The compose file and the two jobs use all of these:

- The S3 API with path-style access, reached by boto3 and by the Java
  `org.apache.iceberg.aws.s3.S3FileIO` inside `apache/iceberg-rest-fixture`.
- Static credentials.
- Bucket creation for `warehouse`, `stac-metadata` and `portolan-docs`.
- **Anonymous public read on a bucket.** The documentation job publishes a
  catalog and reads it with no credentials, which is the claim Portolan makes.
- **Refusal of an anonymous read on a private bucket**, so a bucket left public
  by mistake fails a test.
- Range requests.

The compose file also depends on MinIO's own tooling rather than on S3. `mc mb`
and `mc anonymous set public` create and open the buckets, the docs job waits on
`/minio/health/live`, and the healthcheck runs `mc ready local`.

## Replacements measured on 2026-10-05

Each candidate ran the full stack, including a table written through the REST
fixture by the Java SDK.

| Check | RustFS 1.0.1 | LocalStack 4 | Garage 2.1.0 |
|---|---|---|---|
| `PutBucketPolicy` | pass | pass | `NotImplemented` |
| Anonymous GET on the S3 API port | 200 | 200 | 403 |
| Anonymous range request | 206 | 206 | 206, web port only |
| Private bucket refused anonymously | 403 | 200 | 403 |
| Java `S3FileIO` | pass | pass | pass, needs a checksum setting |
| `/minio/health/live` answers | 200 | no | no |

**RustFS is the replacement.** It passed every check above. It keeps the public
and private distinction the suite relies on. It answers MinIO's health path, so
the docs job and the healthcheck need no change. It is Apache-2.0 and actively
released. Only `mc mb` and `mc anonymous set public` need rewriting.

LocalStack serves a private bucket to an anonymous client, and `ENFORCE_IAM=1`
does not change that, so it would lose one property the suite has today.

Garage implements no bucket policy and serves a public object only through a
separate web port addressed by a Host header, not through the S3 URL. The
documentation job asserts the S3 URL, so Garage would change what it tests.

Rebuilds of MinIO itself stay pullable, `bitnamilegacy/minio` with pinnable tags
and `chainguard/minio` on `latest` only. Both track an archived upstream, so
neither receives a fix again.

## Workaround until it is fixed

None for CI. Locally, run the Iceberg tests that need no Docker:

```bash
uv run pytest tests/iceberg/ -m "not e2e and not e2e_slow"
```
