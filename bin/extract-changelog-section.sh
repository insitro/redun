#!/usr/bin/env bash
# Extract a single version's section from a CHANGELOG, printing to stdout.
# Usage: extract-changelog-section.sh <version> <changelog-path>
# Example: extract-changelog-section.sh 0.46.0 docs/source/CHANGELOG.md
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <version> <changelog-path>" >&2
  exit 2
fi

VERSION="$1"
CHANGELOG="$2"

if [ ! -f "$CHANGELOG" ]; then
  echo "error: changelog not found at $CHANGELOG" >&2
  exit 1
fi

awk -v ver="## $VERSION" '
  $0 == ver {flag=1; next}
  /^## / {if (flag) exit}
  flag {print}
' "$CHANGELOG"
