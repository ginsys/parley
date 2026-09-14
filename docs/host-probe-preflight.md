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
| `claude agents --help` (re-read at `2.1.269`, 2026-09-12) | `--json`: "Print active sessions (interactive and background)"; `--all`: "With --json: also include completed background sessions"; `--cwd <path>`: "Show only background sessions started under <path>". The no-`--all` listing above showed only interactive entries because no background daemon was live, not because `--all` selects a kind |
| `claude --cwd /tmp agents --json --all` (`2.1.269`) | Exit 1: `error: unknown option '--cwd'` — the root `claude` command has no `--cwd`; a background session's directory is the process cwd it is started from |
| `claude logs e9f3bf35` (a `state: "done"` background session) | Exit nonzero: `Couldn't read logs for e9f3bf35 — connect ENOENT /tmp/cc-daemon-1000/9a97f840/control.sock`; the session's daemon has already exited |
| `ls /tmp/cc-daemon-1000/` | Empty except the directory itself — no background session's daemon was live at observation time |
| Codex rollout sample (`$CODEX_HOME/sessions/2026/09/11/rollout-*.jsonl`) | Confirmed real shape: one JSON object per line, a chat turn is `{"type": "response_item", "payload": {"type": "message", "role": "developer"\|"user"\|"assistant", "content": [{"type": "input_text"\|"output_text", "text": ...}]}}`, record-level ISO-8601 `timestamp`. The same file carries `{"type": "event_msg", "payload": {"type": "task_started"\|"task_complete"\|"turn_aborted", ...}}` records, each with its own record-level `timestamp` — the host's own turn boundaries |
| `opencode --pure session list` | Exit 0, no output — no local OpenCode session exists to sample an `export` from |

No message was queued and no session created by this preflight; these are read-only inspections
of already-existing state (help text, plugin cache, a prior completed background session's
listing, an unrelated real Codex rollout file, an empty OpenCode session list). They fill no
matrix cell and are not wake/delivery results.

Confirmed CLI surface beyond what #18 originally named, none of it yet exercised:
- Claude 2.1.268: `--bg`/`--background` paired with `claude attach|agents|logs|stop|rm`,
  `--remote-control [name]`, `--mcp-config` (plain MCP), `--print --input-format=stream-json`.
  `--cwd` exists only on `claude agents` as a listing filter (table above); the runner therefore
  starts its session from the probe directory as the subprocess cwd. Still unexercised: `--cwd`
  on `agents`, `--print`/`--model`/`--max-budget-usd` combined with `--bg`, and `claude --bg`
  itself under captured stdout.
- Codex 0.153.4: `codex queue --thread <uuid|name> --message <text>` (confirmed), also accepts
  `--remote unix://PATH` (unexercised).
- OpenCode 1.18.30: `serve` (headless), `attach <url>`, `session list|delete`, `export
  [sessionID]`, `acp`, `run`.

Conclusion enabled: Claude's `--channels` is a legitimate `unsupported` classification at
`2.1.268` — the mechanism is absent, not merely undocumented — while `claude agents --json` and
`claude logs` looked like a structured alternative for transcript-visibility and turn-start
detection without PTY screen-scraping, *while the background session is still running*. The
stage-1 runner was built on that reading. Superseded 2026-09-13: the live captures below found
`claude logs` to be a raw screen dump, and the trimmed runner reads the session's JSONL
transcript instead (see the [matrix runner](host-probes.md#matrix-runner)).

Two limits of that alternative, both observed here rather than assumed. Nothing in the surface
above submits a message to an *already-running* background session: `--bg` carries its prompt at
creation, `attach` is interactive, and `--remote-control` / `--input-format=stream-json` remain
unexercised — so a Claude submission trial cannot be run at this version until one of those is
actually captured. That is a gap in this runner's tooling, not a demonstrated absence of a host
capability: `--channels` is absent from the help text and the plugin cache, which is evidence
about the mechanism, whereas "we captured no submission path" is evidence about us.

Second, `claude logs` output had **never been read successfully here** — the one attempt failed
with ENOENT against an already-exited daemon (table above). The stage-1 parser in
`scripts/probe/host_trials.py` therefore assumed an untimestamped `User:`/`Assistant:` line
format, and because an untimestamped entry cannot be ordered against a submission instant, it
classified such a read `unobservable` rather than negative. Both the format and the absence of
timestamps were **hypotheses to reproduce against a running background session**, not
observations; the fail-closed handling was what made acting on an unconfirmed format safe.
Superseded 2026-09-13: the section below read `claude logs` from a live session, found no record
structure, and the parser was removed.

## 2026-09-13 — live captures at Claude 2.1.270 / codex-cli 0.154.0 / OpenCode 1.18.30

Unlike the two sections above, these are live captures: each host was actually driven through
session creation, a second message into that session, observation and teardown, under the
operator's real HOME per the 2026-09-11 decision. Every session was created by the capture
itself in a private, empty probe directory (`<probe-cwd>` below; the Codex one was `git init`ed)
and torn down afterwards; no human session was touched. They fill no matrix cell — one run each,
no trial windows, synthetic prompts only — but they replace the 2026-09-11 hypotheses with
observed shapes, and several of those hypotheses were wrong. `claude --version` had drifted again,
to `2.1.270`; `codex --version` now prints `codex-cli 0.154.0`, matching the rollout-recorded
version. All timestamps are UTC.

### Claude Code 2.1.270

| Command/check | Observed result |
| --- | --- |
| `claude --bg --print --model haiku --max-budget-usd 0.05 --session-id <uuid> '<prompt>'` | Exit 1, stdout empty, stderr: ``--bg and --print conflict: --print never starts the interactive session that `claude agents` attaches to, so the job would be unattachable. The prompt is the positional — drop --print: `claude --bg '<task>'`.`` — the stage-1 `create()` argv was wrong. `--max-budget-usd` is a `--print` option, so it is unusable with `--bg`. `--model haiku` was passed instead, but the model row below shows it was not honoured, so no spend bound exists for Claude cells |
| `claude --bg --model haiku --session-id <uuid> '<prompt>'` from `<probe-cwd>` | Exit 0. Stdout, 5 lines: `backgrounded · 69aa52ed` then four indented hint lines (`claude agents`, `claude attach 69aa52ed`, `claude logs 69aa52ed`, `claude stop 69aa52ed`). Stderr: `warning: --bg manages the session id; ignoring --session-id (use --resume <id> to continue an existing session)` and `Starting background service…`. The 2026-09-11 idea of correlating by a caller-chosen `--session-id` is dead: `--bg` ignores it |
| `claude agents --json --all --cwd <probe-cwd>` immediately after | One entry: `{"pid": <n>, "id": "69aa52ed", "cwd": "<probe-cwd>", "kind": "background", "startedAt": 1789292510115, "sessionId": "69aa52ed-1356-4737-afaa-5d03d8e437b9", "name": "<prompt>", "status": "idle", "state": "working"}`. The short `id` is the first 8 hex digits of `sessionId`, so stdout line 1 identifies the session directly and the listing confirms it. Both `status` and `state` are present on a background entry (2026-09-11 saw only `state`, on a finished one); values seen: `status` `idle`/`busy`, `state` `working`/`done`. After the turn: `state: "done"`, `status: "idle"`, `pid` still set — the session stays alive. After `claude stop`: `pid: null`, `status: null`, `state: "done"`. After `claude rm`: the entry is gone |
| `claude logs 69aa52ed`, while `working` and again after `done` | Exit 0 both times, ~7 KB: a raw ANSI terminal screen dump (banner, the prompt line, the reply, the status bar), no timestamps, no record structure. Readable while the daemon lives — the 2026-09-11 ENOENT was a stopped session — but it is not a transcript and this runner does not parse it |
| Session transcript on disk | `$HOME/.claude/projects/<cwd-slug>/<sessionId>.jsonl`, where `<cwd-slug>` is the probe directory's absolute path with every `/` replaced by `-`. One JSON object per line. A prompt is `{"type": "user", "timestamp": "2026-09-13T09:41:50.296Z", "message": {"role": "user", "content": "<prompt>"}, "sessionId": ..., "cwd": ..., "version": "2.1.270", "uuid": ..., "parentUuid": ...}`; a reply is `{"type": "assistant", "timestamp": "2026-09-13T09:42:02.186Z", "message": {"role": "assistant", "content": [{"type": "text", "text": "PONG"}]}, ...}` — `content` is a string on user records and a list of typed parts on assistant records (a preceding assistant record carried only a `thinking` part). Other `type` values seen: `attachment`, `system` (timestamped) and `file-history-snapshot`, `last-prompt`, `mode`, `permission-mode`, `ai-title`, `agent-name`, `custom-title`, `atis-latch`, `bridge-session`, `cost-state` (no `timestamp` key at all). The creation prompt's user record landed 0.18 s after the listing's `startedAt`; the reply 12 s later |
| `claude --bg --resume <sessionId> --model haiku '<msg>'` while the session is running | Exit 0, stdout `backgrounded · e1c973b9` (+hints), stderr: ``note: session 69aa52ed is already running in the background, so this started a copy as e1c973b9. `claude attach 69aa52ed` opens the original.`` — a new session with its own transcript file; nothing reached the original |
| `claude --print --resume <sessionId> --model haiku --max-budget-usd 0.05 '<msg>'` while running | Exit 1, stdout empty, stderr: ``Error: Session 69aa52ed-1356-4737-afaa-5d03d8e437b9 is running as a background session (69aa52ed). Run `claude attach 69aa52ed` to open it, or `claude stop 69aa52ed` first to resume it here. Add --fork-session to branch off a copy instead.`` |
| `claude attach 69aa52ed` under a PTY (24×80, `TERM=xterm-256color`, `PWD=<probe-cwd>`), then a typed line | Prompt ready 3.2 s after launch, judged by 3 s of output silence. Typed `Reply with exactly PARLEY-PROBE-c3c and nothing else.` (45 characters in one write) then Enter (sent separately, 0.5 s later) at 09:50:16.085; the transcript file gained the user record at 09:50:16.099 (seen by the poller +0.27 s) and the assistant text record at 09:50:17.458 (+2.6 s); the screen showed the reply. Ctrl-Z detached (exit 0) and the session stayed listed and running. **This is the only captured path that delivers a message to a live background session** |
| The ready screen of `claude attach`, escapes stripped | The composer is a line holding only `❯` followed by U+00A0 (no-break space), between two full-width rules of `─`; the previous prompt is echoed above it as `❯ <text>` and the reply as `● <text>`. Below: a status bar `[Sonnet 5] │ <last prompt> │ ⌂ claude-…`, `Context █░░ … │ Usage …`, `pid:<n>`, `session:<first 6 hex of the id>`, `⏸ plan mode on (shift+tab to cycle)`. The trimmed runner's readiness regex is the bare-`❯` line; the status bar's `[Sonnet 5]` names the model the attach client shows, not what `--model haiku` asked for (next row) |
| Model actually serving the session created with `--bg --model haiku` | Not Haiku. Every `assistant` transcript record carries `"message": {"model": ...}`: the creation turn and the attach-delivered turn both say `claude-sonnet-5`; the no-flag `--bg --resume` turn (below) says `claude-opus-5`. Whether `--bg` ignores `--model`, or `haiku` is not a resolvable alias, was not established — either way **no spend bound is captured for Claude cells**, and a matrix cell must take its model from the assistant records, not from the creation argv |
| `claude stop 69aa52ed` | Exit 0, stdout `stopped 69aa52ed`; the daemon pid is gone and the listing shows `pid: null, status: null, state: "done"` |
| `claude --bg --resume <sessionId> --model haiku '<msg>'` after `stop` | Exit 0 but again a copy (`backgrounded · e0b268ff`); stderr: `note: background session 69aa52ed keeps its own saved options, so the flags you passed started a copy as e0b268ff. Without flags, the same command continues 69aa52ed itself.` |
| `claude --bg --resume <sessionId> '<msg>'` after `stop`, **no other flags** | Exit 0, stdout `backgrounded · 69aa52ed` — the same short id and `sessionId`; stderr only `Starting background service…`. The original transcript file gained the user record at 09:59:13.546 and the assistant reply at 09:59:14.923, that reply's record naming `claude-opus-5` (an earlier draft of this row inferred "the saved model (Haiku) was kept" from the stderr note about saved options; the record says otherwise). **This is the captured "restarted" path** |
| `claude rm <id>` (after `stop`) | Exit 0, stdout `removed <id>`; the entry leaves the listing. The transcript files persist under `$HOME/.claude/projects/<cwd-slug>/` (three files, one per session started, still present afterwards) |
| Background daemon socket | `/tmp/cc-daemon-<uid>/<hash>/control.sock` while the session lives |

The retained owned Claude transcript was rechecked for record provenance: all seven user and
assistant records, including thinking-only assistant records and the later attach/resume turns,
carry the same full `sessionId` and exact `cwd`. Both fields apply to assistant records as well
as the user-record example above. A filename match alone is insufficient to bind a read.

### codex-cli 0.154.0

| Command/check | Observed result |
| --- | --- |
| `codex exec --json -s read-only --skip-git-repo-check -C <probe-cwd> '<prompt>'` | Exit 0. Stdout, JSONL: `{"type":"thread.started","thread_id":"01a09a24-ff1d-7360-9385-722d230ef92b"}`, `{"type":"turn.started"}`, `{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"PONG"}}`, `{"type":"turn.completed","usage":{...}}`. Stderr: `Reading additional input from stdin...` (stdin was inherited; close it). **A non-interactive creation path with the thread id on stdout exists** — the 2026-09-11 refusal in `CodexDriver.create()` no longer applies |
| Its rollout file | `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-<local-time>-<thread_id>.jsonl`; `session_meta.payload` is `{"id", "session_id", "cwd", "cli_version": "0.154.0", "originator": "codex_exec", "source": "exec", "thread_source", "timestamp"}`. Record types seen: `session_meta`, `turn_context`, `world_state`, `token_usage_record`, `event_msg` (`task_started`, `item_completed`, `token_count`, `task_complete`, `turn_aborted`), `response_item` (`message` with roles `developer`/`user`/`assistant`). A `user` message record also carries the host's own injected text (project instructions, plugin preamble) around the prompt, so the marker must be searched for, never matched whole |
| `codex queue --thread <exec thread> --message 'PARLEY-PROBE-x2 queue test'` with no live session | Exit 0, stdout `Queued message 01a09a25-0f9e-7d52-ad8b-31441d35f687 for thread 01a09a24-ff1d-7360-9385-722d230ef92b.`; nothing delivered. The item is persisted in `$CODEX_HOME/queue_1.sqlite`, table `queued_items(id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms)` with `payload_json` `{"UserInput": ...}` |
| `codex queue --thread '' --message '...'` | Exit 1, stderr `Error: No active session found matching ''.` — the thread argument is checked against known sessions; acceptance is not unconditional |
| `codex --no-alt-screen -s read-only -a never -C <probe-cwd>` (TUI) under a PTY (24×80, `TERM=xterm-256color`), first run in that directory | Prompt `Do you trust the contents of this directory?`; Enter accepted it and **persisted** `[projects."<probe-cwd>"]` / `trust_level = "trusted"` into `$CODEX_HOME/config.toml` — a side effect of any first TUI run in a new probe directory. Composer ready 4.3–5.4 s after launch (3 s of output silence as the criterion); default model `gpt-6-astra high`. The TUI's rollout file (`originator: "codex-tui"`, `source: "cli"`) is created only at its first turn, not at launch, so a typed first prompt is needed before the thread id exists on disk. Pasted text stays in the composer: send the text, pause, then Enter separately |
| The TUI screens, escapes stripped | Trust dialog: `You are in <probe-cwd>` then `Doyoutrustthecontentsofthisdirectory?Workingwithuntrustedcontents…` then `› 1. Yes, continue2.No,quitPress enter to continue` — the words are placed with cursor moves, so they run together once escapes are removed, and the composer placeholder is *already drawn* beneath the dialog. Ready: a box `>_ OpenAI Codex (v0.154.0)` / `model:     gpt-6-astra high   /model to change` / `directory: <probe-cwd>`, then the composer line `› Ask Codex to do anything   ? for shortcuts` (spaces intact) with `gpt-6-astra high · <probe-cwd>` to its right. The trimmed runner's regexes are whitespace-tolerant forms of `Ask Codex to do anything` and `Do you trust the contents of this directory` |
| `codex queue --thread <TUI thread> --message '<marker msg>'` while that TUI sits idle | Exit 0 at 09:59:33.336 (`Queued message ... for thread ...`). Rollout records: `event_msg/task_started` 09:59:40.036, `response_item/message` role `user` 09:59:40.072 (+6.7 s), `response_item/message` role `assistant` 09:59:41.994 (+8.7 s); the screen showed the reply. Queue delivery into a live idle session takes several seconds, not milliseconds |
| `codex queue --thread <exec thread>` while an *unrelated* TUI is live | Exit 0; nothing appeared in the exec thread's rollout within 120 s — the queue is per thread, and a live session only drains its own |
| `codex --no-alt-screen ... -C <probe-cwd> resume <exec thread>` under a PTY | Both earlier queued items (x2 and the mis-targeted x3) were delivered at start, before the composer became ready (15.1 s), each producing a user record, a turn and an assistant reply appended to the *same* rollout file; `queued_items` was empty afterwards. The screen showed the queued text echoed as `› Reply with exactly PARLEY-PROBE-x3 …`, a `Working` spinner, then `• PARLEY-PROBE-x3` above the ready composer `› Ask Codex to do anything` — the placeholder is drawn while a turn runs, so readiness needs the output to go quiet, not just the text to appear. A queued message is therefore delivered by whichever process next serves the thread |
| `codex exec resume <thread> '<msg>'` | Not exercised: the workstation's command policy refused it. Not evidence either way |
| `codex delete <uuid>` non-interactive | Exit 1, stderr `Error: cannot confirm session deletion without an interactive terminal; rerun with --force and a session UUID` |
| `codex delete --force <uuid>` | Exit 0, stdout `Deleted session <uuid>.`; the rollout file is removed. **This is the captured teardown path** |
| `$CODEX_HOME/ipc/` | Does not exist |

### OpenCode 1.18.30

| Command/check | Observed result |
| --- | --- |
| `opencode run --pure --format json --dir <probe-cwd> --title parley-probe-o1 '<prompt>'` (default model) | Stdout one event: `{"type":"error","timestamp":1789292519764,"sessionID":"ses_...","error":{"name":"UnknownError","data":{"message":"Token refresh failed: 401"}}}`; the default model was `openai/gpt-5.6-terra-fast` and its stored credential is stale. A session was still created and listed |
| `opencode models` | No `anthropic` provider is configured (`Error: Provider not found: anthropic` when filtered); providers listed: `google` (43 models), `openai` (21), `opencode` (7). `opencode/ling-3.0-flash-fin-free` is a zero-cost model that works without any stored credential |
| `opencode run --pure --format json --dir <probe-cwd> --title parley-probe-o1b -m opencode/ling-3.0-flash-fin-free '<prompt>'` | Exit 0. Stdout events `step_start`, `text`, `step_finish`, each `{"type", "timestamp": <ms epoch>, "sessionID", "part": {"id", "messageID", "sessionID", "type", ...}}`; the `text` part carries `"text": "PONG"` and `"time": {"start", "end"}`; `step_finish` carries `tokens` and `"cost": 0` |
| `opencode --pure export <sessionID>` | Stderr `Exporting session: <id>`; stdout JSON `{"info": {"id", "slug", "projectID": "global", "directory", "path", "title", "agent", "model": {"id", "providerID", "variant"}, "version": "1.18.30", "summary", "cost", "tokens", "permission", "time": {"created", "updated"}}, "messages": [{"info": {"role": "user", "time": {"created": 1789292850249}, "id", "sessionID", ...}, "parts": [{"type": "text", "text": "<prompt>", ...}]}, {"info": {"role": "assistant", "time": {"created": 1789292850601, "completed": 1789292852828}, "modelID", "providerID", "finish": "stop", ...}, "parts": [{"type": "step-start"}, {"type": "reasoning", "text", "time"}, {"type": "text", "text": "PONG", "time": {"start", "end"}}, {"type": "step-finish", "reason", "tokens", "cost"}]}]}`. All times are millisecond epochs |
| `opencode --pure session list` | A table (`Session ID`, `Title`, `Updated`) over every session on the workstation regardless of directory — a global listing, so ownership must come from the id `run` printed, never from this list |
| `opencode serve --pure --port 43117` | Stdout `Warning: OPENCODE_SERVER_PASSWORD is not set; server is unsecured.` then `opencode server listening on http://127.0.0.1:43117`; `GET /session`, `GET /session/<id>` and `GET /session/<id>/message` return the same JSON shapes as `export` |
| `opencode run --pure --format json --attach http://127.0.0.1:43117 --session <id> -m opencode/ling-3.0-flash-fin-free '<marker msg>'` | Exit 0; the attached client's own stdout carried only a `step_start` event. The export afterwards listed the new user message and an assistant reply whose `time.completed` was about 3 s after the user message's `time.created`. **This is the captured submission path into a live server-held session** |
| `opencode --pure session delete <id>` | Exit 0, stderr (ANSI-coloured) `Session <id> deleted`; `export <id>` afterwards fails with `Error: Session not found: <id>`. Storage is `$HOME/.local/share/opencode/opencode.db` |

The captured OpenCode export also binds every user and assistant message through `info.sessionID`
to the top-level `info.id`. Every part carries that same `sessionID` and a `messageID` equal to
its enclosing message's `info.id`, including `step-start`, `reasoning`, `text` and `step-finish`.
These fields were checked directly in the saved export; they are not inferred from the CLI argument.

Side effects left behind, reported rather than reverted: the Codex trust entry for the probe
directory in `$CODEX_HOME/config.toml`, and the three Claude transcript files under
`$HOME/.claude/projects/<cwd-slug>/` (`claude rm` does not delete them). Both Codex rollouts and
both OpenCode sessions were deleted; the Codex queue table was empty at the end.

Conclusions enabled, each reversing a stage-1 assumption (the runner that implements them is
described under [Matrix runner](host-probes.md#matrix-runner)):
- Claude: create with `claude --bg --model <m> '<prompt>'` and take the id from stdout line 1
  (`backgrounded · <id>`), confirmed against `claude agents --json --all --cwd <probe-cwd>`;
  observe the session's own JSONL transcript by `sessionId`, which is timestamped; submit to a
  live session through `claude attach <id>` under a PTY, or to a stopped one through
  `claude stop <id>` then `claude --bg --resume <sessionId> '<msg>'` with no other flags; tear
  down with `claude stop <id>` then `claude rm <id>`. `claude logs` and `--session-id` play no
  part. The listing-diff and `AmbiguousSessionCreation` machinery built for an uncaptured stdout
  shape is unnecessary. `--model` is passed but establishes nothing about the serving model or
  the spend (row above); the matrix reads the model from the assistant records.
- Codex: create a thread with `codex exec --json` and read `thread.started.thread_id`; submit
  with `codex queue`, into a thread served either by a `codex … resume <thread>` client the run
  opened itself beforehand (the live cells) or by one it opens right after queueing (the
  restarted cell); observe the rollout; tear down with `codex delete --force <uuid>`. The
  provenance walk over every rollout under `$CODEX_HOME/sessions` is replaced by creation
  binding: the runner only ever owns a thread whose id it read from its own `codex exec`
  output. No thread is adopted from a rollout listing, a TUI's own rollout included — a
  first-turn TUI thread has no captured way to bind it to this run before its file exists.
- OpenCode: a free model exists, so the row is runnable at zero cost; `serve` plus
  `run --attach --session` is the live-session submission path, `export` the observation
  channel, `session delete` the teardown. The `OpenCodeDriver` placeholder can be implemented.

At the end of this 2026-09-13 capture, still uncaptured and not assumed by the runner: what a Claude listing shows while
a session is parked on a permission prompt (`approval`), whether `status: "busy"` is reliable for
`busy`, and whether a queued Codex message is delivered mid-turn or only after the running turn
ends. Those are established by the stage-2 trial runs themselves, not assumed here. Likewise
inferred rather than captured, and marked as such in the runner: closing stdin for `codex exec`
(from the `Reading additional input from stdin...` stderr line), `codex exec -m <model>` (the
default model was used), what
`codex delete --force` does to a still-queued item, and whether an OpenCode `error` event or a
nonzero exit takes precedence when both occur.

## Claude listing shapes 2026-09-14

Baseline `3ca5030218a3a33b3c57c9a77118af39ed517185`, Claude Code `2.1.270`, direct disposable
session under real HOME. A plain-MCP compatibility preflight created its own background session
with `claude --bg --model haiku --mcp-config <fixture-config> --strict-mcp-config '<prompt>'`
from an empty private `<probe-cwd>`. The prompt was “Reply with exactly PONG. Do not call tools
or change files.” It invoked `claude attach <short-id>`. MCP initialization was observed,
but the preflight failed before sending a notification; it supplies no wake-matrix result.

The repeated command `claude agents --json --all --cwd <probe-cwd>` returned a background row
plus the invocation's transient interactive registration. Relevant raw shapes, with only
paths, PIDs, display names and identifiers substituted consistently:

```json
[
  {"pid": 101, "id": "11111111", "cwd": "<probe-cwd>", "kind": "background", "startedAt": 1789365313428, "sessionId": "11111111-1111-4111-8111-111111111111", "name": "reply pong instruction", "status": "idle", "state": "done"},
  {"pid": 102, "cwd": "<probe-cwd>", "kind": "interactive", "startedAt": 1789365323003, "sessionId": "22222222-2222-4222-8222-222222222222", "name": "synthetic-probe", "status": "idle"}
]
```

After closing the attach client and `claude stop <short-id>` (exit 0, `stopped <short-id>`),
the same filtered listing contained only this row. `pid` and `status` were absent, not JSON null:

```json
[{"id":"11111111","cwd":"<probe-cwd>","kind":"background","startedAt":1789365312488,"sessionId":"11111111-1111-4111-8111-111111111111","name":"reply pong instruction","state":"done"}]
```

The prior parser refused both shapes and therefore stopped before removing the background row.
Its creation output, exact full UUID, private cwd and stopped state were rechecked before
finishing cleanup with `claude rm <short-id>`. That exited 0; the filtered listing became `[]`.
Hook-generated files remained in the disposable cwd and were retained with the failed attempt.

The runner now validates and excludes the distinct interactive shape without minting it. For
background rows, both absent live fields are normalized to null only in the captured states
described here; a partial omission or malformed identity still refuses the listing.
Controlled fixtures reproduce both failures and verify that removal remains confined to the
created background ID. This expands the earlier captured contract; it does not adopt a session
from a listing or infer a permission-prompt shape.

A second fresh session with the same command/configuration captured the startup race: its first
listing had the same background identity fields, `state: "working"`, and neither `pid` nor
`status`. This is persisted metadata before a live PID is published, not proof of a stopped
process. Cancelling with `claude stop <short-id>` returned exit 0 and changed that row to
`state: "stopped"`, still without the two live fields. The exact created UUID/cwd was checked
before removal; `claude rm` exited 0 and the filtered listing became `[]`.

Creation therefore accepts this metadata shape and continues its bounded wait for `done`.
Cleanup checks state as well as PID: a surviving `working` row never authorizes removal, even
when the stop command returned 0. Both `done` and the newly captured `stopped` state can confirm
stopping when there is no live PID. Controlled startup and stop/removal fixtures cover the
distinction. The failed second attempt also sent no notification and fills no matrix cell.

### Local launcher invalidates the apparent attach

Two subsequent busy-state preflights never called the configured MCP hold tool. The second
retained the final client screen: its conversation began with the user text `attach`, and the
assistant later reported that the hold tool was absent. The background transcript still contained
only the original PONG exchange. The installed launcher prepended `--enable-auto-mode
--autocompact 300000 --effort high` to every terminal invocation, including the `attach`
subcommand. The resulting interactive conversation was distinct from the created background
session. A composer alone did not prove attachment to the requested session.

The listing shapes above remain captured, but the interactive row is not evidence of a valid
background attachment. An intervening idle-MCP preflight sent its notification and observed no
marker in the background transcript for 125 seconds; its apparent attached-client precondition
was affected by the same launcher and is not a valid attached-idle matrix trial. All these failed
preconditions remain separate from the three-trial results. Their created background sessions
were removed and their held client processes closed. No session from a listing was adopted.

### Native attach, busy completion and approval records

The MCP reproducer checks both the PATH launcher and the native attachment executable with
`--version` before creating any session. Both must report `2.1.270 (Claude Code)`; a missing
or differently versioned native executable refuses the attempt before host creation.

A new capture invoked the native `claude attach <short-id>` inside a systemd user scope with
the launcher's unchanged limits (`MemoryHigh=6G`, `MemoryMax=12G`, `MemorySwapMax=4G`,
`CLAUDE_MEM_SCOPE=1`). Its screen showed the existing PONG exchange, the background process PID
and the created session's six-character display prefix. The subsequently typed request appeared
in that exact background transcript. This established the attachment that the wrapper invocation
had failed to provide; no launcher or global configuration was edited.

The hold-tool request reached a permission menu even with `--allowedTools
mcp__parleyprobe__hold` on creation. The inherited session was in plan mode. The menu showed
`parleyprobe — Hold Tool: (MCP)`, `Do you want to proceed?`, `1. Yes`, `3. No`, and
`Esc to cancel`. No choice was answered. The actual hold call was not yet persisted in the
transcript or received by the server; the exact preceding user request, menu and absence of
`hold_started` together establish this precondition. Tool discovery had already emitted:

```json
{"type":"assistant","timestamp":"2026-09-14T06:42:16.832Z","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_synthetic","name":"ToolSearch","input":{"query":"select:mcp__parleyprobe__hold","max_results":3},"caller":{"type":"direct"}}]}}
{"type":"user","timestamp":"2026-09-14T06:42:16.863Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_synthetic","content":[{"type":"tool_reference","tool_name":"mcp__parleyprobe__hold"}]}]}}
```

These newly captured shapes initially made the ordinary message parser report an unreadable
transcript. It now validates the bounded ToolSearch/reference exchange without treating tool
arguments or references as message text. Assistant tool use can establish assistant activity;
the reference result cannot establish transcript visibility or acknowledgement. Unknown tool
result content and malformed fields still make the read unobservable. The original approval
run retains its first parser-limited trial and its second failed setup separately from any rerun.
That second screen's ANSI-stripped footer read `Ec to cancel`; further failed setups showed
`Ec to canel` and `Do you want to proced?`. Cursor movement reused unchanged characters that
escape stripping lost. Enumerating truncated text variants was unreliable. Approval detection
therefore reconstructs the current 80×24 terminal with pinned [pyte 0.8.2](https://pypi.org/project/pyte/0.8.2/)
and `wcwidth 0.2.13`, using the documented [screen and byte-stream API](https://pyte.readthedocs.io/en/latest/api.html).
Both are investigation/test dependencies, not Go runtime dependencies. The renderer refuses a
truncated byte history. Controlled cursor-reuse and screen-clear tests ensure it preserves text
and does not keep a vanished menu. Detection still requires the captured tool label, question,
Yes/No choices, exact bound request and absence of tool execution. The initial idle-composer
readiness pattern is unchanged. All failed setup artifacts are retained separately.

The rendered capture also established that the pending `mcp__parleyprobe__hold` tool-use record
may be persisted after the permission menu appears, even though the server has not received the
call. Its shape is the same typed assistant call with `input: {}` and `caller: {type: "direct"}`.
The parser recognizes this synthetic call without exposing its arguments as message text.
While the verified approval menu remains present, this is the existing blocked turn's activity,
not a new turn caused by the notification. The final approval trials apply that distinction;
earlier parser-limited runs remain in the [attempt ledger](evidence/host-wake/attempts-20260914.json).

For busy state, the same number-generation prompt used by Codex produced a user record at
`2026-09-14T06:36:55.007Z` and a descendant `system` / `turn_duration` record at
`2026-09-14T06:37:16.075Z`, with `durationMs: 21016`. The listing changed from `working` to
`done` while the same background PID remained live. The notification was requested while
`working`. The orchestration follows the exact busy user record's `parentUuid` ancestry to
completion, checks the session UUID/cwd, and excludes the existing turn's assistant tail from
new-turn evidence. Host wall timestamps and local first-observation times remain separate.

### Resume retains saved MCP options

A stopped idle session received a synthetic control-file message while its MCP server was gone.
The existing no-flags `claude --bg --resume <full-uuid> '<PONG-prompt>'` command then printed:

```text
backgrounded · af28df35 · reply protocol test
  claude agents             list sessions
  claude attach af28df35    open in this terminal
  claude logs af28df35      show recent output
  claude stop af28df35      stop this session
```

Its stderr reported that it woke the same session with saved `--mcp-config`,
`--strict-mcp-config` and `--model` options. A second fixture process initialized. The previous
first-line parser refused the display-name suffix; cleanup still removed the owned original
session. This failed preflight supplies the resume output/configuration shape, not a completed
restart matrix cell. The parser now accepts a title only after the captured second `·` separator,
and still extracts only the first line's exact short ID. Controlled tests reject an unseparated
suffix. Full UUID/cwd checks and copy detection remain in place.

## Codex state captures 2026-09-14

Installed and rollout-recorded version: `codex-cli 0.154.0`. Each capture used a fresh private
Git directory, real HOME, a driver-created thread, and controlled cleanup. Creation was
`codex exec --json -s read-only --skip-git-repo-check -C <probe-cwd> '<PONG-prompt>'`
with stdin closed. The TUI command was `codex --no-alt-screen -s read-only -a never
-C <probe-cwd> resume <thread-id>`; the captured trust dialog was answered only for that new cwd.

A disconnected cleanup preflight queued one synthetic message with
`codex queue --thread <thread-id> --message '<marker-message>'`, then ran
`codex delete --force <thread-id>` while the item was pending. A read-only query of the queue
database, restricted to that exact created thread UUID, counted one item before deletion and
zero afterward. The rollout disappeared and ownership was empty. This permits a deliberate
disconnected investigation without resuming merely to drain pending cleanup data.

The busy precondition used this prompt in the owned ready TUI:

> This is an isolated busy-state preflight. Write the integers from one through two hundred in
> English words, one per line, followed by HOLD COMPLETE. Do not use tools or change files.

The raw rollout emitted `event_msg` / `task_started` at `2026-09-14T06:11:13.685Z` and
`task_complete` at `2026-09-14T06:11:41.345Z`, with the same `payload.turn_id`: 27.660 seconds.
The orchestration requires a pending start before submission and verifies afterward that
submission fell strictly inside that exact turn's interval. A prior request for a timed tool
sleep was rejected because the child's approval policy was `never`; it did not establish the
intended busy tool state and is retained as a failed precondition.

The approval capture changed only the child TUI invocation to
`codex -c 'approvals_reviewer="user"' --no-alt-screen -s read-only -a on-request
-C <probe-cwd> resume <thread-id>`. It requested an escalated synthetic command
`python3 -c 'print("PARLEY_APPROVAL_PREFLIGHT")'`. The client displayed:

```text
Would you like to run the following command?
Environment: local
Reason: ... nah requires review of this command before execution.
$ python3 -c 'print("PARLEY_APPROVAL_PREFLIGHT")'
1. Yes, proceed (y)
2. No, and tell Codex what to do differently (esc)
Press enter to confirm or esc to cancel
```

The bound rollout contained the corresponding `response_item` / `custom_tool_call`, name
`exec`, with no matching `custom_tool_call_output`. No approval was answered. A matrix trial
requires both this complete menu and the unresolved call, and sends its marker through the
external queue command. The child-specific approval configuration and local review hook are
part of the result; no global configuration was changed.

Both `$CODEX_HOME/ipc/ipc.sock` and
`$CODEX_HOME/app-server-control/app-server-control.sock` were absent before creation and while
an owned TUI was ready. Earlier `codex app-server daemon version` reported ENOENT for the latter.
No daemon was started and no other session was enumerated. This is scoped reachability evidence,
not proof that Codex has no IPC mechanism.

`scripts/probe/codex_matrix.py` reproduces three trials for each of `idle`, `busy`, `approval`,
`disconnected` and `restarted`. It preserves individual observations, UTC and monotonic journal
times, raw rollout/client evidence and failed attempts, then applies the fixed trial protocol.
Run it explicitly with `--state <state> --output-directory <new-private-evidence-directory>`.
Ordinary tests inject every process creator; they do not launch a host.
