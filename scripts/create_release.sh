#!/usr/bin/env bash
set -euo pipefail

# Determine repository root (superproject or submodule)
REPO_ROOT="$(git rev-parse --show-toplevel)"
SUBMODULE_ROOT="$REPO_ROOT"
if [[ -d "$REPO_ROOT/ts-connect" ]]; then
  SUBMODULE_ROOT="$REPO_ROOT/ts-connect"
fi

cd "$SUBMODULE_ROOT"

VERSION_FILE="VERSION"
RELEASE_FILE="RELEASE"

if [[ ! -f "$VERSION_FILE" ]]; then
  echo "VERSION file not found at $SUBMODULE_ROOT/$VERSION_FILE" >&2
  exit 1
fi

VERSION="$(tr -d '\n\r ' < "$VERSION_FILE")"
if [[ -z "$VERSION" ]]; then
  echo "VERSION file is empty" >&2
  exit 1
fi

if git rev-parse "$VERSION" >/dev/null 2>&1; then
  echo "Tag $VERSION already exists" >&2
  exit 1
fi

# Ensure only VERSION/RELEASE are staged/modified
mapfile -t STATUS_ENTRIES < <(git status --porcelain | cut -c4-)
if [[ ${#STATUS_ENTRIES[@]} -eq 0 ]]; then
  echo "No changes to release." >&2
  exit 1
fi
for file in "${STATUS_ENTRIES[@]}"; do
  case "$file" in
    VERSION|RELEASE) ;;
    *)
      echo "Working tree contains changes outside VERSION/RELEASE: $file" >&2
      exit 1
      ;;
  esac
done

GIT_ARGS=("$VERSION_FILE")
RELEASE_LABEL=""
if [[ -f "$RELEASE_FILE" ]]; then
  RELEASE_LABEL="$(tr -d '\n\r' < "$RELEASE_FILE")"
  [[ -n "$RELEASE_LABEL" ]] && GIT_ARGS+=("$RELEASE_FILE")
fi

COMMIT_MESSAGE="chore(release): $VERSION"

git add "${GIT_ARGS[@]}"
git commit -m "$COMMIT_MESSAGE"

git tag -a "$VERSION" -m "Release $VERSION"

git push origin HEAD
git push origin "$VERSION"

if command -v gh >/dev/null 2>&1; then
  GH_FLAGS=("--generate-notes")
  if [[ -n "$RELEASE_LABEL" ]]; then
    GH_FLAGS+=("--title" "$VERSION - $RELEASE_LABEL")
  else
    GH_FLAGS+=("--title" "$VERSION")
  fi
  if [[ "$VERSION" =~ -(alpha|beta|rc) ]]; then
    GH_FLAGS+=("--prerelease")
  fi
  gh release create "$VERSION" "${GH_FLAGS[@]}"
else
  echo "gh CLI not found; skipped GitHub release creation." >&2
fi
