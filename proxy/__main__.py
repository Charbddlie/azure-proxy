"""Run the proxy in the foreground. start.sh nohups this.

No daemonising and no second mode. Backgrounding is start.sh's job with
nohup; a process that forks itself is one that cannot be supervised by
anything that did not expect it to.

The dashboard is a separate, read-only program (see tui/) that watches this
one over its own HTTP surface. It used to be able to run in here on a thread,
which meant the proxy's lifetime was tied to a terminal — the wrong tradeoff
for something every client on the box points at.

    python -m proxy
    uvicorn proxy.server:app --host 127.0.0.1 --port 8787
"""

import atexit

import uvicorn

from .server import app, cfg, clear_pidfile, write_pidfile

write_pidfile()
atexit.register(clear_pidfile)

# uvicorn's access log stays on. It is the only thing that shows requests to
# paths the proxy has no route for — which is exactly how a client pointed at a
# face that does not exist yet announces itself.
uvicorn.run(app, host=cfg.host, port=cfg.port, log_level=cfg.log_level)
