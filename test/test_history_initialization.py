"""RPM threshold boundaries, backup, and read-only initialization preview."""

import json
import sqlite3
import unittest

from tools.initialize_others_history import initialize


class HistoryInitializationTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.execute("CREATE TABLE state(name TEXT PRIMARY KEY, payload TEXT)")
        self.saved = dict(cursor=123, states={
            "small": dict(safe_rpm=27, other_rpm=20, foreign=.9, foreign_seen=True),
            "below": dict(safe_rpm=99.9, foreign_seen=True),
            "boundary": dict(safe_rpm=100, foreign_seen=True),
            "large": dict(safe_rpm=500, foreign_seen=True),
            "unknown": dict(safe_rpm=0, foreign_seen=True),
            "clear": dict(safe_rpm=20, foreign_seen=False)})
        self.db.execute("INSERT INTO state VALUES('checkpoint',?)", (json.dumps(self.saved),))
        self.db.commit()

    def checkpoint(self):
        return json.loads(self.db.execute("SELECT payload FROM state WHERE name='checkpoint'").fetchone()[0])

    def test_preview_preserves_database(self):
        result = initialize(self.db)
        self.assertEqual(set(result["changed"]), {"small", "below"})
        self.assertEqual(self.checkpoint(), self.saved)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM state").fetchone()[0], 1)

    def test_apply_changes_flags_only_and_preserves_backup(self):
        result = initialize(self.db, apply=True)
        current = self.checkpoint()
        for key, state in self.saved["states"].items():
            expected = dict(state, foreign_seen=False) if key in ("small", "below") else state
            self.assertEqual(current["states"][key], expected)
        self.assertEqual(current["cursor"], 123)
        backup = json.loads(self.db.execute("SELECT payload FROM state WHERE name=?", (result["backup"],)).fetchone()[0])
        self.assertTrue(backup["routes"]["small"]["foreign_seen"])
        self.assertEqual(backup["threshold_rpm"], 100)
        self.assertEqual(initialize(self.db, apply=True)["changed"], {})

    def test_backup_failure_rolls_back_initialization(self):
        self.db.execute("CREATE TRIGGER reject_backup BEFORE INSERT ON state BEGIN SELECT RAISE(ABORT, 'backup failed'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            initialize(self.db, apply=True)
        self.assertEqual(self.checkpoint(), self.saved)


if __name__ == "__main__":
    unittest.main()
