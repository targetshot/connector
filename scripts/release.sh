#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <version> [release-label]" >&2
  exit 1
fi

VERSION="$1"
RELEASE_LABEL="${2:-}"

for cmd in git gh jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: '$cmd' is required but not found in PATH." >&2
    exit 1
  fi
done

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

function maybe_commit() {
  local message=$1
  if git diff --cached --quiet; then
    echo "No staged changes for '$message'; skipping commit."
  else
    git commit -m "$message"
  fi
}

function wait_for_workflow() {
  local workflow="$1"
  local target_sha="$2"
  local threshold_epoch="$3"

  echo "Waiting for workflow '$workflow' on commit $target_sha ..."
  while true; do
    local run_json run_info run_id
    run_json="$(gh run list \
      --workflow "$workflow" \
      --limit 20 \
      --json databaseId,headSha,headBranch,status,conclusion,createdAt)"

    run_info="$(echo "$run_json" | jq -r --arg sha "$target_sha" --argjson threshold "$threshold_epoch" '
      map(select(.headSha == $sha and .headBranch == "main" and (.createdAt | fromdateiso8601) >= $threshold))
      | first // empty')"

    if [[ -n "$run_info" ]]; then
      run_id="$(echo "$run_info" | jq -r '.databaseId')"
      gh run watch --exit-status "$run_id"
      break
    fi

    sleep 10
  done
}

echo "Syncing development branch…"
git fetch origin
git checkout development
git pull --ff-only origin development
git add -A
maybe_commit "chore: prepare changes for $VERSION"
git push origin development

echo "Syncing main branch…"
git checkout main
git pull --ff-only origin main
git merge --no-ff development -m "Merge development for $VERSION" || true

echo "Bumping VERSION/RELEASE…"
./scripts/bump_version.sh "$VERSION" "$RELEASE_LABEL"
git add VERSION
if [[ -n "$RELEASE_LABEL" && -f RELEASE ]]; then
  git add RELEASE
fi
maybe_commit "chore: bump ts-connect to $VERSION"

HEAD_SHA="$(git rev-parse HEAD)"
push_epoch="$(date -u +%s)"
git push origin main

wait_for_workflow "Build & publish ts-connect" "$HEAD_SHA" "$push_epoch"

echo "Tagging release $VERSION …"
git tag -f "$VERSION" "$HEAD_SHA"
git push -f origin "$VERSION"

promote_epoch="$(date -u +%s)"
echo "Triggering stable promotion for $VERSION…"
gh workflow run "Promote ts-connect image" --ref main -f source="$VERSION" -f channel=stable >/dev/null

wait_for_workflow "Promote ts-connect image" "$HEAD_SHA" "$promote_epoch"

echo "Release $VERSION completed and promoted to stable."
