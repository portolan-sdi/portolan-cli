#!/bin/bash
# Auto-fetch API docs for core dependencies (ADR-0013)
# Triggered by PostToolUse on Read tool
#
# Detects imports of geoparquet_io, rio_cogeo, or obstore in Python files
# and injects their current API documentation using gitingest.

set -euo pipefail

# Read hook input from stdin
INPUT=$(cat)

# Extract file path from hook input
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# Only process Python files
if [[ ! "$FILE_PATH" =~ \.py$ ]]; then
    exit 0
fi

# Generate session ID from terminal/process info for caching
SESSION_ID="${CLAUDE_SESSION_ID:-$(echo "$$-$(date +%Y%m%d)" | md5sum | cut -d' ' -f1)}"

# Run the Python script to detect imports and fetch docs
# Pass the full hook input via stdin
cd "$CLAUDE_PROJECT_DIR"

RESULT=$(echo "$INPUT" | CLAUDE_SESSION_ID="$SESSION_ID" python3 scripts/fetch_lib_docs.py 2>/dev/null || true)

# If we got a result, output it
if [[ -n "$RESULT" ]]; then
    echo "$RESULT"
fi

exit 0
