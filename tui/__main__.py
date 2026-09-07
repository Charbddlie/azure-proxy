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

from .app import DEFAULT_SCROLL_LINES, run


def _default_url() -> str:
    """Read host and port out of settings/policy.yaml if it is to hand.

    Falls back to the shipping default rather than failing: someone attaching
    from another machine has no settings tree, and being wrong about the port
    is a connection error they can see and fix, not a reason to refuse to start.
    """
    try:
        import os
        import yaml
        from proxy.config import ROOT
        here = ROOT
        with open(os.path.join(here, "settings", "policy.yaml")) as f:
            server = (yaml.safe_load(f) or {}).get("server") or {}
        return "http://{}:{}".format(server.get("host", "127.0.0.1"),
                                     server.get("port", 8811))
    except Exception:
        return "http://127.0.0.1:8811"


def _scroll_lines(value) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise argparse.ArgumentTypeError("scroll lines must be a positive integer")
    try:
        value = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("scroll lines must be a positive integer")
    if value < 1:
        raise argparse.ArgumentTypeError("scroll lines must be a positive integer")
    return value


def _default_scroll_lines() -> int:
    import os
    import yaml
    from proxy.config import ROOT
    try:
        with open(os.path.join(ROOT, "settings", "policy.yaml")) as file:
            policy = yaml.safe_load(file) or {}
    except OSError:
        return DEFAULT_SCROLL_LINES
    except yaml.YAMLError:
        raise argparse.ArgumentTypeError("cannot read tui.scroll_lines from settings/policy.yaml")
    settings = policy.get("tui", {}) if isinstance(policy, dict) else {}
    if settings is None:
        settings = {}
    if not isinstance(settings, dict):
        raise argparse.ArgumentTypeError("tui settings must be a mapping")
    return _scroll_lines(settings.get("scroll_lines", DEFAULT_SCROLL_LINES))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tui", description=__doc__.splitlines()[0])
    parser.add_argument("--attach", action="store_true",
                        help="accepted and ignored; watching is all this does")
    parser.add_argument("--url", default=None,
                        help="proxy base URL (default: from settings/policy.yaml)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds between polls (default: 1)")
    parser.add_argument("--scroll-lines", type=_scroll_lines, default=None,
                        help="lines per wheel event (default: tui.scroll_lines in settings/policy.yaml, or 2)")
    args = parser.parse_args(argv)
    try:
        scroll_lines = args.scroll_lines if args.scroll_lines is not None else _default_scroll_lines()
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))

    from proxy.config import ROOT
    from urllib.parse import urlsplit
    default = _default_url()
    url = args.url or default
    local = ROOT if url.rstrip("/") == default.rstrip("/") and urlsplit(url).hostname in (
        "127.0.0.1", "localhost", "0.0.0.0", "::1") else None
    run(url, interval=args.interval, local_root=local, scroll_lines=scroll_lines)
    return 0


if __name__ == "__main__":
    sys.exit(main())
