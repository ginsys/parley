#!/usr/bin/env python3
"""Inventory exact identifier bytes on a stopped, checkpointed SQLite copy."""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

# Fixed identifiers only; no input is interpolated into SQL.
FIELDS = (
    ("conversations", "id", ("id", "name")),
    ("grants", "conversation", ("conversation", "peer_a_id", "peer_b_id")),
    ("envelopes", "id", ("conversation", "from_peer", "to_peer")),
)


def check_sidecars(path):
    for suffix in ("-wal", "-journal"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError("nonempty WAL/journal: require a stopped, checkpointed copy")


def inventory(path, output):
    """Return the number of findings. Never initialize, migrate or write SQLite."""
    path = path.resolve(strict=True)
    if not path.is_file():
        raise ValueError("database must be an existing regular file")
    check_sidecars(path)
    count = 0
    # Immutable avoids even SQLite's read-only WAL/SHM bookkeeping. It is safe
    # only under the documented stopped-copy precondition; it cannot detect a
    # live writer or manufacture a consistent backup.
    db = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    db.text_factory = bytes
    try:
        db.execute("PRAGMA query_only=ON")
        for table, key, fields in FIELDS:
            for column in fields:
                version = "grant_version" if table == "grants" else "NULL"
                sql = (
                    f"SELECT rowid, CAST({key} AS BLOB), {version}, "
                    f"typeof({column}), CAST({column} AS BLOB) FROM {table} ORDER BY rowid"
                )
                for rowid, row_key, grant_version, kind, raw in db.execute(sql):
                    if (
                        kind == b"text"
                        and raw
                        and any(b != 0x20 for b in raw)
                        and all(0x20 <= b <= 0x7E for b in raw)
                    ):
                        continue
                    record = {
                        "table": table,
                        "column": column,
                        "rowid": rowid,
                        "key_hex": None if row_key is None else row_key.hex(),
                        "grant_version": grant_version,
                        "storage_type": kind.decode("ascii"),
                        "value_hex": None if raw is None else raw.hex(),
                        "value_escaped": None if raw is None else repr(raw),
                    }
                    output.write(json.dumps(record, ensure_ascii=True) + "\n")
                    count += 1
        check_sidecars(path)
    finally:
        db.close()
    output.write(json.dumps({"findings": count}) + "\n")
    return count


def main(argv=None, output=None, errors=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path, help="stopped, checkpointed database copy")
    args = parser.parse_args(argv)
    output = sys.stdout if output is None else output
    errors = sys.stderr if errors is None else errors
    try:
        return 1 if inventory(args.database, output) else 0
    except (OSError, sqlite3.Error, ValueError) as exc:
        # repr keeps hostile filenames/SQLite identifiers from forging output lines.
        errors.write(f"inventory incomplete: {str(exc)!r}\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
