# Identifier compatibility inventory

Conversation names and peer IDs accept printable ASCII bytes `0x20`–`0x7E`, with at least one
non-space byte. Accepted spaces and punctuation remain exact. Unicode and malformed UTF-8 IDs
are incompatible; message bodies are unaffected. This is the implemented
[identifier rule](architecture.md#accepted-identifier-alphabet).

`scripts/identifier_inventory.py` reads identifier bytes without decoding repair or store
initialization. It covers both `conversations.id` and `conversations.name`, every grant version's
conversation and both peers, and every envelope's conversation and both endpoints, regardless of
lifecycle state. Non-TEXT storage also reports a finding rather than treating a BLOB as a TEXT key.
It does not inspect message bodies, change history, enroll/revoke/renew, migrate schemas or repair
identities. A clean inventory is not evidence of other migration, authentication or host readiness.

## Prepare the input

A human must stop every writer and obtain a complete, checkpointed copy with the committed data
in the database file. Use a trusted private backup location with permissions appropriate for
plaintext messages. Keep the deployment and its original sidecars together; never delete WAL/SHM
or rollback journals to make an inventory run succeed. If a backup still has pending WAL or journal
state, preserve it and use a reviewed SQLite backup/recovery procedure to obtain the checkpointed
copy first. Do not start an empty replacement database.

Run only against that stopped copy, never the live deployment. The tool rejects nonempty adjacent
`-wal` or `-journal` files so it cannot silently skip committed WAL history or required rollback.
It uses SQLite `mode=ro&immutable=1` and query-only reads to avoid modifying even WAL/SHM bookkeeping.
[SQLite's immutable contract](https://www.sqlite.org/uri.html#uriimmutable) disables locking and
change detection: the caller must ensure the file stays unchanged for the whole scan. Sidecar
checks cannot prove a live writer is absent. This tool neither makes nor validates a backup.

From the repository root, with the pinned project Python:

```sh
mise exec -- python3 scripts/identifier_inventory.py /path/to/checkpointed-copy.db
```

Help requires no database. The supplied path must exist; URI metacharacters in filenames are
escaped, and a missing file is never created. Unsupported schemas or I/O errors return an
incomplete result, never a clean summary.

## Interpret the result

Each finding is one ASCII JSON line with `table`, `column`, `rowid`, `key_hex`, `grant_version`,
`storage_type`, `value_hex` and `value_escaped`. The key is the conversation ID for conversation and
grant rows and the envelope ID for envelope rows; the version distinguishes historical grants.
Hex preserves exact bytes, including NUL, line breaks, invalid UTF-8 and punctuation. SQL NULL is
represented by JSON null, distinct from empty bytes. The escaped field is Python's byte-literal
representation for inspection only, never an expression to execute. Rowids locate records in this
copy; they are not durable identities for a later administration operation.

The final line is `{"findings": N}`. Exit status is `0` for a complete scan with no findings, `1`
for a complete scan with findings, or `2` for incomplete/invalid input. Treat earlier finding lines
as partial when the exit status is `2`; do not infer cleanliness from absent output.

For example, distinct malformed keys `61ff` and `61fe` remain distinct in this output. Go JSON
would otherwise encode both strings using the same replacement character. Do not copy those
replacement characters into a new identity or discard the original bytes.

If no incompatible IDs exist, this alphabet change requires no identifier migration. If findings
exist, retain the evidence and agree their disposition before moving that database behind a
text-only administration interface. Current exact-key human revocation remains available;
incompatible grants cannot be renewed or authorize new work. Queued delivery rejects without a
state rewrite or budget claim; already claimed attempts retain normal settlement. No automatic
history deletion, alias mapping or new recovery API is provided.

## Verification

`mise run verify` runs the inventory fixtures through `mise run python`, alongside the Go
identifier/authorization tests. Fixtures cover clean and historical grants, every inventoried
column, malformed keys, non-TEXT/blank/Unicode values, pending WAL/journal rejection, missing files,
unknown schemas, URI metacharacters and unchanged database/sidecar bytes. The evidence uses only
disposable synthetic databases; no operator database has been inventoried by this change.
