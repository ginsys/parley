"""Byte-preserving inventory fixtures; no operator database is opened."""

import importlib.util
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("identifier_inventory", SCRIPTS / "identifier_inventory.py")
inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inventory)


class IdentifierInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "copy ?#.db"
        db = sqlite3.connect(self.path)
        db.executescript((SCRIPTS.parent / "internal/store/schema.sql").read_text())
        db.execute("INSERT INTO conversations VALUES ('c', 'c', 'time')")
        db.execute("INSERT INTO grants VALUES ('c',1,'a','b','bidirectional',5,0,'time',NULL,'revoked',NULL)")
        db.execute("INSERT INTO grants VALUES ('c',2,'a','b','bidirectional',5,0,'time',NULL,'active',NULL)")
        db.execute("INSERT INTO envelopes (id,conversation,from_peer,to_peer,text,grant_version,state,created_at,updated_at) VALUES ('e','c','a','b','secret payload',1,'acked','time','time')")
        db.commit()
        db.close()

    def run_inventory(self):
        before = {p.name: p.read_bytes() for p in self.path.parent.iterdir()}
        out, err = io.StringIO(), io.StringIO()
        code = inventory.main([str(self.path)], out, err)
        after = {p.name: p.read_bytes() for p in self.path.parent.iterdir()}
        self.assertEqual(before, after, "inventory modified source or sidecars")
        return code, [json.loads(line) for line in out.getvalue().splitlines()], err.getvalue()

    def test_clean_history(self):
        self.assertEqual(self.run_inventory(), (0, [{"findings": 0}], ""))

    def test_all_columns_and_historical_versions_preserve_raw_bytes(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE conversations SET name=CAST(? AS TEXT)", (b"name\xff",))
            db.execute("UPDATE grants SET peer_a_id=CAST(? AS TEXT) WHERE grant_version=1", (b"a\xff",))
            db.execute("UPDATE grants SET peer_b_id=CAST(? AS TEXT) WHERE grant_version=2", (b"a\xfe",))
            db.execute("UPDATE envelopes SET from_peer=CAST(? AS TEXT), to_peer=?", (b"a\x00\n|", "\ufffd"))
        code, records, err = self.run_inventory()
        self.assertEqual((code, err, records[-1]), (1, "", {"findings": 5}))
        self.assertEqual({r["value_hex"] for r in records[:-1]}, {"6e616d65ff", "61ff", "61fe", "61000a7c", "efbfbd"})
        for record in records[:-1]:
            self.assertEqual(record["key_hex"], "65" if record["table"] == "envelopes" else "63")
        self.assertNotIn("secret payload", str(records))
        self.assertEqual({r["grant_version"] for r in records if r.get("table") == "grants"}, {1, 2})

    def test_conversation_keys_at_every_location(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE conversations SET id=CAST(? AS TEXT)", (b"c\xff",))
            db.execute("UPDATE grants SET conversation=CAST(? AS TEXT)", (b"c\xff",))
            db.execute("UPDATE envelopes SET conversation=CAST(? AS TEXT)", (b"c\xff",))
        code, records, err = self.run_inventory()
        self.assertEqual((code, err, records[-1]), (1, "", {"findings": 4}))
        self.assertTrue(all(r["value_hex"] == "63ff" for r in records[:-1]))

    def test_empty_blank_unicode_and_nontext(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE conversations SET name=' '")
            db.execute("UPDATE grants SET peer_a_id='',peer_b_id='café'")
            db.execute("UPDATE envelopes SET from_peer=?", (b"ascii-blob",))
        code, records, _ = self.run_inventory()
        self.assertEqual((code, records[-1]), (1, {"findings": 6}))
        self.assertTrue(any(r.get("storage_type") == "blob" for r in records))

    def test_nonempty_wal_or_journal_rejected_without_reading_partial_history(self):
        for suffix in ("-wal", "-journal"):
            sidecar = Path(str(self.path) + suffix)
            sidecar.write_bytes(b"pending committed or rollback state")
            code, records, err = self.run_inventory()
            self.assertEqual((code, records), (2, []))
            self.assertIn("stopped, checkpointed", err)
            sidecar.unlink()

    def test_missing_database_is_not_created(self):
        missing = self.path.parent / "missing.db"
        out, err = io.StringIO(), io.StringIO()
        self.assertEqual(inventory.main([str(missing)], out, err), 2)
        self.assertFalse(missing.exists())

    def test_unknown_schema_is_incomplete_not_clean(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("DROP TABLE envelopes")
        code, records, err = self.run_inventory()
        self.assertEqual(code, 2)
        self.assertFalse(any("findings" in r for r in records))
        self.assertIn("incomplete", err)


if __name__ == "__main__":
    unittest.main()
