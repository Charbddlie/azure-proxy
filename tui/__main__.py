"""The proxy's dashboard. A reader, and only a reader.

    python -m tui                       watch the proxy on the configured port
    python -m tui --url http://…:8811   watch one somewhere else

It starts nothing, stops nothing and writes nothing — every endpoint it touches
(/healthz, /routes, /events) is a reader, so opening and closing it does not
touch the service. `./start_tui.sh` is this with a couple of checks in front of
it; the proxy itself is managed by start.sh / stop.sh / restart.sh.
"""

import argparse
import sys

from .app import run


def _default_url() -> str:
    """Read host and port out of settings/policy.yaml if it is to hand.

    Falls back to the shipping default rather than failing: someone attaching
    from another machine has no settings tree, and being wrong about the port
    is a connection error they can see and fix, not a reason to refuse to start.
    """
    try:
        import os
        import yaml
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(here, "settings", "policy.yaml")) as f:
            server = (yaml.safe_load(f) or {}).get("server") or {}
        return "http://{}:{}".format(server.get("host", "127.0.0.1"),
                                     server.get("port", 8811))
    except Exception:
        return "http://127.0.0.1:8811"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tui", description=__doc__.splitlines()[0])
    parser.add_argument("--attach", action="store_true",
                        help="accepted and ignored; watching is all this does")
    parser.add_argument("--url", default=None,
                        help="proxy base URL (default: from settings/policy.yaml)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds between polls (default: 1)")
    args = parser.parse_args(argv)

    run(args.url or _default_url(), interval=args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
