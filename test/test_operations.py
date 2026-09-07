"""Clock scheduling, log isolation and real-terminal unattended redraw."""

from datetime import datetime, timezone
import fcntl
import logging
import os
import pty
import select
import struct
import subprocess
import tempfile
import termios
import time
import unittest
from unittest.mock import patch

from proxy.logfiles import handler, prune_logs
from tools.scheduled_restart import next_run, restart
from test_split import fixture
from run_tests import PYTHON, ROOT, sticky_ask


class OperationsTests(unittest.TestCase):
    def test_next_beijing_0100_and_day_boundary(self):
        before = datetime(2026, 9, 7, 16, 59, tzinfo=timezone.utc)
        self.assertEqual(next_run(before).isoformat(), "2026-09-08T01:00:00+08:00")
        self.assertEqual(next_run(before.replace(hour=17)).isoformat(), "2026-09-09T01:00:00+08:00")

    def test_legacy_schedule_requires_explicit_migration(self):
        with patch("tools.scheduled_restart.health", return_value={"ok": True}), \
                patch("tools.scheduled_restart.subprocess.run") as run:
            with self.assertRaisesRegex(RuntimeError, "legacy"):
                restart(30)
            run.assert_not_called()

    def test_scheduled_roll_upgrades_routing_first_and_checks_health(self):
        with patch("tools.scheduled_restart.health", return_value={
                "ok": True, "supervisor": {"active": 123}, "routing": {"ok": True}}), \
                patch("tools.scheduled_restart.subprocess.run") as run:
            restart(30)
            self.assertEqual([c.args[0][3:5] for c in run.call_args_list],
                             [["restart", "routing"], ["restart", "serving"]])

    def test_log_retention_is_limited_to_rotated_diagnostics(self):
        with tempfile.TemporaryDirectory() as root:
            names = ("proxy.log.2026-09-01_01", "routing.log.2026-09-01_01",
                     "affinity.sqlite3", "proxy.log", "other.backup")
            for name in names:
                with open(os.path.join(root, name), "w") as file:
                    file.write("keep or expire")
                os.utime(os.path.join(root, name), (1000, 1000))
            prune_logs(root, now=100000)
            self.assertEqual(set(os.listdir(root)), set(names[2:]))
            output = handler(root, "serving")
            output.emit(logging.LogRecord("test", logging.INFO, "", 0, "new line", (), None))
            output.close()
            with open(os.path.join(root, "proxy.log")) as file:
                self.assertIn("new line", file.read())

    def test_real_terminal_repaints_without_input_at_all_widths(self):
        with fixture() as (p, a, b):
            master, slave = pty.openpty()
            original = termios.tcgetattr(slave)
            process = subprocess.Popen([PYTHON, "-m", "tui", "--url", p.url(""), "--interval", "0.1"],
                                       cwd=ROOT, stdin=slave, stdout=slave, stderr=slave,
                                       env=dict(os.environ, TERM="xterm-256color"))
            captured = b""
            try:
                for width in (60, 80, 120, 180):
                    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, width, 0, 0))
                    until = time.monotonic() + .65
                    start = len(captured)
                    while time.monotonic() < until:
                        if select.select([master], [], [], .1)[0]:
                            captured += os.read(master, 65536)
                    self.assertGreater(len(captured), start)
                    self.assertIsNone(process.poll())
                os.write(master, b"q")
                self.assertEqual(process.wait(timeout=5), 0)
                self.assertEqual(termios.tcgetattr(slave), original)
                self.assertIn(b"serving", captured)
                self.assertIn(b"routing", captured)
                self.assertNotIn(b"Traceback", captured)
                self.assertEqual(sticky_ask(p)[0], 200)
            finally:
                if process.poll() is None:
                    process.kill(); process.wait()
                os.close(master); os.close(slave)


if __name__ == "__main__":
    unittest.main()
