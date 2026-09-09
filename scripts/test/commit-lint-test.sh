#!/usr/bin/env bash
# Fixture tests for scripts/ci/commit-lint.sh: every accepted and rejected subject shape.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
lint="$here/../ci/commit-lint.sh"
pass=0
fail=0

expect() {
  local want=$1 subject=$2 rc=0
  "$lint" --subject "$subject" >/dev/null 2>&1 || rc=$?
  if [ "$rc" -eq "$want" ]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    echo "FAIL: expected rc=$want got rc=$rc for: $subject" >&2
  fi
}

expect 0 'feat: add thing'
expect 0 'fix(api): handle nil'
expect 0 'feat(ci)!: drop old input'
expect 0 'docs!: rewrite'
expect 0 'chore(deps): bump actions/checkout'
expect 0 'release: v0.1.0'
expect 1 'Merge branch main into feature'   # no merge exemption: range mode skips real merges
expect 1 'Add thing'
expect 1 'feat add thing'
expect 1 'feat:'
expect 1 'feat: '
expect 1 'feature: add thing'
expect 1 'FEAT: add thing'
expect 1 'fix(api) handle nil'
expect 1 'wip'
expect 1 ''

# Range mode, against this repository's own history.
expect_range() {
  local want=$1 base=$2 head=$3 rc=0
  "$lint" "$base" "$head" >/dev/null 2>&1 || rc=$?
  if [ "$rc" -eq "$want" ]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    echo "FAIL: expected rc=$want got rc=$rc for range: $base..$head" >&2
  fi
}
expect_range 2 0000000000000000000000000000000000000000 HEAD   # unreachable base: walk fails, never green
expect_range 2 HEAD no-such-ref                                # bad head, same
expect_range 0 HEAD HEAD                                       # empty range: nothing to lint, not an error

echo "commit-lint-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
