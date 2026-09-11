# Host probe preflight — 2026-09-10

Source baseline: `a8beeffd1bbbfaf0033e086e68af74c5108cb56a`. These are command-availability
observations on the development workstation, not wake or delivery results. The
[trial protocol](host-probes.md#trial-protocol) still requires disposable host sessions and
separate outcomes for every trial.

| Command/check | Observed result |
| --- | --- |
| `claude --version` | Exit 0: `2.1.267 (Claude Code)` |
| `codex --version` | Exit 0: `codex-cli 0.153.4`; warning about inability to create PATH aliases in the restricted environment |
| `opencode --version` | Exit 0: `1.18.30` |
| `codex queue --help` | Exit 0; advertises queuing a message for an existing session using thread and message arguments |
| `claude --help` | Exit 0; MCP config options advertised; Channels absent from this help output, which does not establish lack of support |
| `opencode auth --help` | Exit 0; provider-list command advertised |
| `opencode --pure auth list` (17:20 UTC, restricted environment) | Exit 1 while opening its normal log file; provider listing not obtained, no credential values retained |
| `opencode --pure auth list` (17:27 UTC, retry with normal log access) | Exit 0; two stored credential entries reported; no credential values retained or provider request made |
| Expected Codex IPC path existence | No socket found at the configured/default Codex HOME's `ipc/ipc.sock`; no connection attempted |
| `command -v herdr` | Not found; harness does not depend on herdr |

Commands had a ten-second timeout, except provider listing with fifteen seconds. None timed out.
The initial provider/socket observations were recorded at 17:20 UTC; the provider retry is listed
separately above and was also recorded in [issue #18](https://github.com/ginsys/parley/issues/18).
The retry resolves the listing failure, but credential presence does not prove usable provider
authentication in a disposable session. No message was queued, no ordinary
session was contacted and no provider request was made. Presence of a command is not evidence
that it wakes a session, and absence of an IPC path in this environment is not product-wide
unsupported status. OpenCode remains the intended third host; verify disposable-session authentication or
record an explicit owner scope amendment before treating the investigation as complete.

The controlled harness fixtures are independently reproducible with `mise run python`; their
success does not fill any real-host result cell. [Issue #18](https://github.com/ginsys/parley/issues/18)
owns the remaining investigation evidence and acceptance.

## 2026-09-11 — version drift and confirmed mechanism surface

One day after the observations above, `claude --version` reports `2.1.268 (Claude Code)`, not
`2.1.267`; a same-day `codex` rollout's own `session_meta` record independently reports
`"cli_version":"0.154.0"` even though `codex --version` on this workstation still prints
`codex-cli 0.153.4`. Neither observation changes conclusions drawn at the earlier pin — it
demonstrates why every matrix cell must carry its own exact version rather than inherit one from
a preflight run on a different day.

| Command/check | Observed result |
| --- | --- |
| `claude --version` | Exit 0: `2.1.268 (Claude Code)` |
| `claude --help` | No `--channels` flag anywhere in the option list (`grep -in channel` on the full help text: no match) |
| Official plugin marketplace cache, `external_plugins/` | Contains `asana context7 discord fakechat firebase github gitlab imessage laravel-boost linear playwright serena telegram terraform`; no channels plugin |
| `claude agents --json --all` | Background sessions report `{id, kind: "background", state, cwd, sessionId, name, startedAt}`; `state` values seen: `"done"` |
| `claude agents --json` (no `--all`) | Interactive sessions report a *different* schema: `{pid, kind: "interactive", status, cwd, sessionId, name, startedAt}` — `status`, not `state` |
| `claude logs e9f3bf35` (a `state: "done"` background session) | Exit nonzero: `Couldn't read logs for e9f3bf35 — connect ENOENT /tmp/cc-daemon-1000/9a97f840/control.sock`; the session's daemon has already exited |
| `ls /tmp/cc-daemon-1000/` | Empty except the directory itself — no background session's daemon was live at observation time |
| Codex rollout sample (`~/.codex/sessions/2026/09/11/rollout-*.jsonl`) | Confirmed real shape: one JSON object per line, a chat turn is `{"type": "response_item", "payload": {"type": "message", "role": "developer"\|"user"\|"assistant", "content": [{"type": "input_text"\|"output_text", "text": ...}]}}`, record-level ISO-8601 `timestamp`. The same file carries `{"type": "event_msg", "payload": {"type": "task_started"\|"task_complete"\|"turn_aborted", ...}}` records, each with its own record-level `timestamp` — the host's own turn boundaries |
| `opencode --pure session list` | Exit 0, no output — no local OpenCode session exists to sample an `export` from |

No message was queued and no session created by this preflight; these are read-only inspections
of already-existing state (help text, plugin cache, a prior completed background session's
listing, an unrelated real Codex rollout file, an empty OpenCode session list). They fill no
matrix cell and are not wake/delivery results.

Confirmed CLI surface beyond what #18 originally named, none of it yet exercised:
- Claude 2.1.268: `--bg`/`--background` paired with `claude attach|agents|logs|stop|rm`,
  `--remote-control [name]`, `--mcp-config` (plain MCP), `--print --input-format=stream-json`.
- Codex 0.153.4: `codex queue --thread <uuid|name> --message <text>` (confirmed), also accepts
  `--remote unix://PATH` (unexercised).
- OpenCode 1.18.30: `serve` (headless), `attach <url>`, `session list|delete`, `export
  [sessionID]`, `acp`, `run`.

Conclusion enabled: Claude's `--channels` is a legitimate `unsupported` classification at
`2.1.268` — the mechanism is absent, not merely undocumented — while `claude agents --json` and
`claude logs` give a structured alternative for transcript-visibility and turn-start detection
without PTY screen-scraping, *while the background session is still running*. See the [matrix
runner](host-probes.md#matrix-runner) for how `scripts/probe/host_trials.py` uses this.

Two limits of that alternative, both observed here rather than assumed. Nothing in the surface
above submits a message to an *already-running* background session: `--bg` carries its prompt at
creation, `attach` is interactive, and `--remote-control` / `--input-format=stream-json` remain
unexercised — so a Claude submission trial cannot be run at this version until one of those is
actually captured. That is a gap in this runner's tooling, not a demonstrated absence of a host
capability: `--channels` is absent from the help text and the plugin cache, which is evidence
about the mechanism, whereas "we captured no submission path" is evidence about us.

Second, `claude logs` output has **never been read successfully here** — the one attempt failed
with ENOENT against an already-exited daemon (table above). The parser in
`scripts/probe/host_trials.py` therefore assumes an untimestamped `User:`/`Assistant:` line
format, and because an untimestamped entry cannot be ordered against a submission instant, it
classifies such a read `unobservable` rather than negative. Both the format and the absence of
timestamps remain **hypotheses to reproduce against a running background session**, not
observations; the fail-closed handling is what makes acting on an unconfirmed format safe.
