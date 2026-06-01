#!/usr/bin/env bash
# Iterate all version tags, create a GitHub Release for any that don't have one.
# Pinned to --repo insitro/redun-private so a misconfigured remote or a stray
# GH_REPO env var cannot cause writes to the wrong repository.
# Run locally with `gh auth status` confirmed.
# Safe to re-run: skips tags that already have releases.
#
# Usage: backfill-github-releases.sh [--dry-run]
#   --dry-run: prints what would be created without calling `gh release create`.
#              Existence checks via `gh release view` still hit the API (reads only).
set -euo pipefail

DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then
  DRY_RUN=1
  shift
fi

REPO=insitro/redun-private
REPO_ROOT=$(git rev-parse --show-toplevel)
CHANGELOG="$REPO_ROOT/docs/source/CHANGELOG.md"
EXTRACT="$REPO_ROOT/bin/extract-changelog-section.sh"

if [ ! -x "$EXTRACT" ]; then
  echo "error: $EXTRACT not found or not executable" >&2
  exit 1
fi

TMPNOTES=$(mktemp)
trap 'rm -f "$TMPNOTES"' EXIT

# Filter to semver-shaped tags only. The glob '[0-9]*' would also match a
# future tag like '2026-archive' and waste an extract call per tag.
for tag in $(git tag -l '[0-9]*' | grep -E '^[0-9]+\.[0-9]+(\.[0-9]+)?$' | sort -V); do
  # Per-tag existence check via `gh release view` rather than a single
  # `gh release list --limit N`: exact, no pagination cap, no regex match
  # surprises (a previous version used `grep -qx "$tag"` which treated `.`
  # as a regex wildcard).
  if gh release view "$tag" --repo "$REPO" >/dev/null 2>&1; then
    echo "skip=$tag reason=already_released"
    continue
  fi
  "$EXTRACT" "$tag" "$CHANGELOG" > "$TMPNOTES"
  if ! grep -q '[^[:space:]]' "$TMPNOTES"; then
    echo "skip=$tag reason=no_changelog_or_empty"
    continue
  fi
  if [ "$DRY_RUN" -eq 1 ]; then
    notes_lines=$(wc -l < "$TMPNOTES" | tr -d ' ')
    echo "would_create=$tag notes_lines=$notes_lines"
    echo "--- notes preview (first 5 lines) ---"
    head -5 "$TMPNOTES"
    echo "---"
    continue
  fi
  echo "create=$tag"
  # --latest=false: backfill must not steal the Latest pointer. gh's default
  # of --latest=auto evaluates by release creation time, so an old tag
  # backfilled NOW would become Latest. Promote the desired Latest with
  # `gh release edit X.Y.Z --latest=true --repo $REPO` after the run if
  # the workflow has not already created the top-version release.
  gh release create "$tag" \
    --repo "$REPO" \
    --title "$tag" \
    --notes-file "$TMPNOTES" \
    --verify-tag \
    --latest=false
done
if [ "$DRY_RUN" -eq 1 ]; then
  echo "act=backfill status=ok mode=dry-run"
else
  echo "act=backfill status=ok"
fi
