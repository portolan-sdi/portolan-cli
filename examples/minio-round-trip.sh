#!/bin/sh
# Push the local example to MinIO and clone the published catalog.
set -eu

: "$CATALOG_DIR"
: "$CLONED_CATALOG_DIR"
: "$PORTOLAN_EXAMPLE_SOURCE"
: "$PORTOLAN_S3_ENDPOINT"
: "$AWS_ACCESS_KEY_ID"
: "$AWS_SECRET_ACCESS_KEY"

REMOTE="s3://portolan-docs/catalog"

./examples/local-publishing.sh
portolan push "$REMOTE" --collection places --catalog "$CATALOG_DIR"
portolan clone "$REMOTE" "$CLONED_CATALOG_DIR"
