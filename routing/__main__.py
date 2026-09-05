"""Run the independently restartable routing process in the foreground."""

import logging
import signal
import threading

from proxy.config import ROOT
from proxy.bridge import INTERVAL
from proxy.process import ProcessClaim
from .engine import Engine


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    claim = ProcessClaim(ROOT, "routing")
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    engine = None
    try:
        engine = Engine(ROOT)
        while not stop.is_set():
            engine.step()
            stop.wait(INTERVAL)
        engine.step(ready=False)
    finally:
        if engine:
            engine.close()
        claim.close()


if __name__ == "__main__":
    main()
