"""Run serving in the foreground against the last published routing snapshot.

The management scripts provide background operation. Routing runs separately
via python -m routing and can be restarted while this process keeps serving.
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
