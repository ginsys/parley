# Contributing

Work from a GitHub issue with an explicit objective, scope, exclusions, deliverables, acceptance
criteria and verification method. Accept responsibility before assigning yourself. Refine
incomplete work before implementation; an issue's existence is not implementation readiness.

## Authority

| Information | Authority |
| --- | --- |
| Work scope, acceptance criteria, ownership and completion | GitHub issue |
| Blocking relationships | Native GitHub dependencies |
| Core invariants and the protected-controller boundary | [AGENTS.md](AGENTS.md) |
| Implemented contracts, architecture and limitations | [Architecture](docs/architecture.md) |

Do not maintain parallel status checklists in repository documents or chat. Issue acceptance
criteria belong in the issue; native dependencies alone define blockers. Permanent decisions must
not exist only in chat. Accepted design changes that affect this repository's invariants update
`AGENTS.md` and the architecture document in the same change as their implementation.

## Triage and labels

Use the work-item form. Select investigation, decision, specification, implementation or
validation; during triage apply exactly one corresponding `type/investigation`, `type/decision`,
`type/specification`, `type/implementation` or `type/validation` label. A form dropdown does not
dynamically apply a label.

During triage, require investigations to describe alternatives and the decision their evidence
enables. That field is optional at submission so other work types can omit it; incomplete
investigations need refinement before work starts. Outcome links are optional and describe enabled
decisions or artifacts, never a second blocker list.

The label set is managed from [ginsys/.github](https://github.com/ginsys/.github)
(`standards/labels.json`); its daily audit reports drift and a manual apply deletes labels it does
not know. Issue open/closed state is authoritative for completion. Use `status/in-progress` when
accepted work is underway and `status/needs-review` when it awaits review; these two labels are
mutually exclusive. Use `status/needs-refinement` while implementation detail or acceptance
verification remains incomplete. It may coexist with either workflow label. Add one or more `area/`
labels during triage for the component or repository concern the work touches; a new `area/` value
is created live to unblock triage and back-filled into the label registry in the same unit of work.

## Evidence and closure

Investigations compare alternatives and identify the decision their evidence enables. Record exact
tool versions, synthetic inputs, reproduction commands, expected/observed outcomes, failure cases,
limitations and recommendations. A negative finding can close an investigation; resulting decisions
and remediation remain separately tracked.

Implementation follows finalized contracts and includes meaningful verification of applicable
success, rejection, interruption and recovery behavior — including the fixtures named in
`AGENTS.md` (impersonation, approval-forgery command shapes, budget exhaustion, revoke-vs-dispatch,
crash-after-handoff, stale grant version, reply-marker malformed/duplicate/wrong-recipient/stale,
readiness handshake delayed/timeout/reconnect). Update code and related documentation together.
Closure requires every specified acceptance criterion, the required evidence and landed artifacts,
assessed by the owner or designated reviewer. An open draft PR or a local passing check alone is
not closure evidence.

## Change and review workflow

1. Prepare a scoped change on a feature branch; preserve unrelated work and keep the default branch
   unchanged.
2. Run the [documentation checks](#documentation-checks) and any checks required by the issue, then
   review the actual diff against `AGENTS.md` and `docs/architecture.md`. Record exact checks and
   limitations.
3. Open a draft PR stating the problem, scope, linked issues, validation evidence and outstanding
   limitations.
4. Have the owner or designated reviewer assess correctness, scope, safety, evidence and
   documentation. Record findings and resolve them or explicitly record an accepted disposition.
5. Complete required evidence before seeking readiness or merge authorization. The owner may merge
   their own PR after review. Agents require explicit authorization to change PR readiness, merge
   or change branch protection.

Every PR runs the `CI` workflow (documentation checks, `actionlint`, `go build`/`go vet`/`go
test`/`gofmt`, conventional-commit subjects, action pins, Python lint and probe fixtures; aggregated as the `checks` context) and a
Claude review (`PR Review`) whose findings are advisory (`PR_REVIEW_THREADS_MODE` is unset, so it
posts one comment and creates no resolvable threads) but whose completion is a **required check**:
the `main-protection` ruleset
(defined in the `github_repos.parley` entry in [ginsys/.github](https://github.com/ginsys/.github)'s
settings policy) requires `checks` and `pr-review / AI Code Review`, blocks direct pushes and
deletion of `main`, requires linear history, and requires every review thread resolved. Merges go
through a **merge queue** (REBASE, one entry at a time): `gh pr merge --auto` enqueues the PR rather
than merging it directly. Do not claim CI success without an actual run result. Review and merge are
separate actions.

## Documentation checks

With mise installed, run from the repository root. The docs and Python tasks share an isolated,
gitignored `.venv`, created with the pinned interpreter and `requirements-dev.txt`; CI uses the
same setup. A system Python or globally installed PyYAML is not a prerequisite:

```sh
mise run docs
git diff --check
git diff --cached --check
```

With [mise](https://mise.jdx.dev/) installed and `origin/main` fetched, `mise run verify` runs
everything the `CI` workflow runs: the documentation check, `actionlint` and `shellcheck`, the
commit-lint fixture tests, the whole-tree whitespace check, `go build`/`go vet`/`go test`/`gofmt`,
Ruff on all Python scripts and the controlled probe/CI-gate fixtures,
the conventional-commit check on this branch's commits, and the action-pin check (which clones
go-kure/.github into the gitignored `upstream/`).

The verifier reads repository files as UTF-8 and reports file locations relative to the repository
root. It checks root guidance (`README.md`, `CONTRIBUTING.md`, `AGENTS.md`, `CLAUDE.md`) and any
Markdown under `docs/`: inline relative links and anchors, local equivalents of this repository's
`blob/main` document links, balanced fenced blocks and trailing whitespace. It also checks
issue-form YAML, field names/types/requiredness, disabled blank issues and the CLAUDE delegation.

Link checks cover inline links without titles or spaces in their destinations. Titled and
reference-style links are skipped and need manual review. Trailing whitespace is rejected,
including Markdown's two-space hard line breaks; use a paragraph break instead. The verifier does
not fetch external links, verify live tracker state, fully lint Markdown or validate design
semantics. Review changed external references and the actual diff separately; for a committed PR,
also run `git diff --check <base-commit>...HEAD` using its actual base commit.
