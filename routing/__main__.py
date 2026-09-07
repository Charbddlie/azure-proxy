"""Run the independently restartable routing process in the foreground."""

import logging
import signal
import sqlite3
import threading

from proxy.config import ROOT
from proxy.bridge import INTERVAL
from proxy.process import ProcessClaim
from proxy.state import is_busy
from .engine import Engine


def run(root, stop):
    engine = None
    waiting = False
    log = logging.getLogger(__name__)
    try:
        while not stop.is_set():
            try:
                if engine is None:
                    engine = Engine(root)
                engine.step()
            except sqlite3.OperationalError as error:
                if not is_busy(error):
                    raise
                if not waiting:
                    log.warning("routing database busy; retrying with consumed progress preserved: %s",
                                error)
                waiting = True
            else:
                if waiting:
                    log.info("routing database recovered; publication resumed")
                waiting = False
            stop.wait(INTERVAL)
        if engine is not None:
            try:
                engine.step(ready=False)
            except sqlite3.OperationalError as error:
                if not is_busy(error):
                    raise
                log.warning("routing stopped while database busy; last heartbeat will expire")
    finally:
        if engine is not None:
            engine.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    claim = ProcessClaim(ROOT, "routing")
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    try:
        run(ROOT, stop)
    finally:
        claim.close()


if __name__ == "__main__":
    main()
