#!/usr/bin/env bash
# Conventional-commit subject check for the commits a PR adds.
# Usage: scripts/ci/commit-lint.sh <base-sha> <head-sha>
#        scripts/ci/commit-lint.sh --subject '<subject>'   (single subject, used by the tests)
set -euo pipefail

# Merge commits are not exempted here: range mode walks with --no-merges, so a subject starting
# with "Merge " on an ordinary commit is just a non-conventional subject.
PATTERN='^(feat|fix|docs|refactor|perf|test|build|ci|chore|release)(\([a-zA-Z0-9_./-]+\))?!?: .+$'

check_subject() {
  if [[ "$1" =~ $PATTERN ]]; then
    return 0
  fi
  return 1
}

if [ "${1:-}" = "--subject" ]; then
  if check_subject "${2:-}"; then
    echo "ok: ${2:-}"
    exit 0
  fi
  echo "not a conventional commit subject: ${2:-}" >&2
  exit 1
fi

if [ $# -ne 2 ]; then
  echo "usage: $0 <base-sha> <head-sha> | --subject '<subject>'" >&2
  exit 2
fi

base=$1
head=$2
# Walk first, outside any pipeline or process substitution, so an unreachable revision (a
# force-pushed-away SHA, a typo) fails the check instead of silently linting nothing.
if ! commits=$(git log --no-merges --format='%H %s' "$base..$head"); then
  echo "cannot walk $base..$head" >&2
  exit 2
fi
failed=0
checked=0
while IFS= read -r line; do
  [ -n "$line" ] || continue
  sha=${line%% *}
  subject=${line#* }
  checked=$((checked + 1))
  if check_subject "$subject"; then
    echo "ok   ${sha:0:8} $subject"
  else
    echo "FAIL ${sha:0:8} $subject" >&2
    failed=$((failed + 1))
  fi
done <<< "$commits"

echo "checked $checked commit(s), $failed failing"
[ "$failed" -eq 0 ]
