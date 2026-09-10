"""Run the independently restartable routing process in the foreground."""

import logging
import os
import signal
import sqlite3
import threading

from proxy.config import ROOT
from proxy.bridge import INTERVAL
from proxy.process import ProcessClaim
from proxy.state import is_busy
from proxy.state import Store
from proxy.retention import CLEANUP_INTERVAL
from .engine import Engine
from .discovery import RouteRefresher


def run(root, stop):
    engine = None
    refresher = None
    waiting = False
    log = logging.getLogger(__name__)
    try:
        while not stop.is_set():
            try:
                if engine is None:
                    engine = Engine(root)
                    if engine.config.route_refresh_enabled:
                        refresher = RouteRefresher(root, engine.config.route_refresh_interval,
                                                   engine.config.route_refresh_timeout)
                        refresher.start()
                if refresher:
                    discovery = refresher.take()
                    if discovery is not None:
                        try:
                            engine.reload_routes(discovery)
                            refresher.accepted()
                        except Exception as error:
                            refresher.rejected(error)
                    engine.route_refresh = refresher.status()
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
        if refresher:
            refresher.close()
        if engine is not None:
            engine.close()


def main():
    handlers = None
    if os.environ.get("AZURE_PROXY_MANAGED_LOG"):
        from proxy.logfiles import handler
        output = handler(ROOT, "routing")
        output.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        handlers = [output]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)
    claim = ProcessClaim(ROOT, "routing")
    if handlers:
        from proxy.logfiles import release_bootstrap_stdio
        release_bootstrap_stdio()
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    maintenance = threading.Thread(target=maintain, args=(ROOT, stop), daemon=True)
    maintenance.start()
    try:
        run(ROOT, stop)
    except Exception:
        logging.getLogger(__name__).exception("routing process failed")
        raise
    finally:
        stop.set()
        maintenance.join(timeout=2)
        claim.close()


def maintain(root, stop):
    """Bounded diagnostic cleanup is outside the publication/startup path."""
    while not stop.wait(CLEANUP_INTERVAL):
        store = None
        try:
            store = Store(root)
            while store.expire() and not stop.wait(0.1):
                pass
            store.reclaim()
            from proxy.logfiles import prune_logs
            prune_logs(root)
        except (sqlite3.Error, OSError):
            logging.getLogger(__name__).exception("diagnostic cleanup will retry")
        finally:
            if store:
                store.close()


if __name__ == "__main__":
    main()
