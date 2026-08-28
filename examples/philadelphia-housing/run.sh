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
