"""The dashboard itself: header, board, footer, and the keys that move between.

Two clocks, deliberately different. Data is fetched once a second on the poller's
thread — the numbers behind it move on the scale of a load window, and polling
faster would only make the proxy answer questions about itself. The screen
repaints four times a second, which is what makes a keystroke feel immediate
rather than something that happens up to a second later.

Input is read with `select` on a raw stdin in the same loop as the repaint, not
on a thread. A thread would have to hand keystrokes across, and the only thing
it would buy is the ability to block on read — which is exactly what must not
happen, because the screen has to keep repainting while nothing is being typed.
"""

import os
import select
import signal
import sys
import termios
import tty
from typing import List

from rich.console import Console, Group as RichGroup
from rich.live import Live
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from . import theme
from .bars import legend
from .boards import (BOARD_TITLES, BOARDS, DEFAULT_EVENT_FILTER, EVENT_FILTERS,
                     EVENT_KIND_FILTERS, filter_events, render_events, render_groups,
                     render_trapi)
from .client import Poller
from .snapshot import SORTS, Snapshot

REDRAW_HZ = 4.0


class Dashboard:

    def __init__(self, poller: Poller, console: Console):
        self.poller = poller
        self.console = console
        self.board = 0
        self.offset = [0] * len(BOARDS)
        self.sort = 0
        self.filter = DEFAULT_EVENT_FILTER
        self.kind_filter = 0
        self.show_all = False
        self.quit = False
        self._extent = [0] * len(BOARDS)

    # -- keys -------------------------------------------------------------
    def key(self, seq: str) -> None:
        if seq in ("q", "Q", "\x03"):           # Ctrl-C arrives as a byte here
            self.quit = True
        elif seq in ("\x1b[C", "l", "\t"):
            self.board = (self.board + 1) % len(BOARDS)
        elif seq in ("\x1b[D", "h"):
            self.board = (self.board - 1) % len(BOARDS)
        elif seq in ("\x1b[B", "j"):
            self._scroll(1)
        elif seq in ("\x1b[A", "k"):
            self._scroll(-1)
        elif seq in ("\x1b[6~", " "):
            self._scroll(10)
        elif seq == "\x1b[5~":
            self._scroll(-10)
        elif seq in ("g", "\x1b[H"):
            self.offset[self.board] = 0
        elif seq in ("G", "\x1b[F"):
            self.offset[self.board] = max(0, self._extent[self.board] - 1)
        elif seq == "s":
            self.sort = (self.sort + 1) % len(SORTS)
        elif seq == "f":
            self.filter = (self.filter + 1) % len(EVENT_FILTERS)
            self.offset[2] = 0
        elif seq == "t":
            self.kind_filter = (self.kind_filter + 1) % len(EVENT_KIND_FILTERS)
            self.offset[2] = 0
        elif seq == "a":
            self.show_all = not self.show_all
            # The list just got longer or shorter under the cursor. Keeping the
            # old offset would leave it pointing at a row that is no longer
            # there, which reads as the board having jumped on its own.
            self.offset[0] = self.offset[1] = 0
        elif seq == "r":
            self.poller.refresh_now()

    def _scroll(self, delta: int) -> None:
        limit = max(0, self._extent[self.board] - 1)
        self.offset[self.board] = max(
            0, min(limit, self.offset[self.board] + delta))

    # -- rendering --------------------------------------------------------
    def render(self):
        snapshot = Snapshot(self.poller.snapshot())
        width = self.console.width
        # header (3) + tabs (1) + rule (1) + footer (2)
        body_height = max(4, self.console.height - 7)

        if BOARDS[self.board] == "trapi":
            body, extent = render_trapi(
                snapshot, width, body_height, self.offset[self.board])
        elif self.board == 2:
            body, extent = render_events(
                snapshot.events, width, body_height, self.offset[2],
                self.filter, snapshot.dropped, self.kind_filter)
        elif self.board == 1:
            body, extent = render_groups(
                snapshot.models, width, body_height, self.offset[1],
                SORTS[self.sort], snapshot, "model", self.show_all)
        else:
            body, extent = render_groups(
                snapshot.sources, width, body_height, self.offset[0],
                SORTS[self.sort], snapshot, "source", self.show_all)
        self._extent[self.board] = extent
        if BOARDS[self.board] == "trapi":
            self.offset[self.board] = min(self.offset[self.board], max(0, extent - 1))

        return RichGroup(_header(snapshot, BOARDS[self.board] == "trapi"), _tabs(self.board, snapshot,
                                                  self.show_all, self.filter,
                                                  self.kind_filter),
                         Rule(style=theme.BORDER), body,
                         _footer(self, snapshot, extent))

    # -- loop -------------------------------------------------------------
    def run(self) -> None:
        interval = 1.0 / REDRAW_HZ
        with Live(self.render(), console=self.console, screen=True,
                  refresh_per_second=REDRAW_HZ, transient=False) as live:
            while not self.quit:
                for seq in _read_keys(interval):
                    self.key(seq)
                live.update(self.render())


# --------------------------------------------------------------------------
# chrome
# --------------------------------------------------------------------------

def _header(snapshot: Snapshot, trapi: bool = False) -> Table:
    health = snapshot.health
    table = Table.grid(padding=(0, 2), expand=True)
    table.add_column(ratio=1)
    table.add_column(justify="right")

    left = Text()
    left.append("azure-proxy", style="bold {}".format(theme.TITLE))
    host, port = health.get("host"), health.get("port")
    if host:
        left.append("  {}:{}".format(host, port), style=theme.LABEL)
    if trapi:
        left.append("  provider ", style=theme.DIM)
        left.append("TRAPI", style=theme.ACCENT)
    elif snapshot.balance:
        left.append("  balance ", style=theme.DIM)
        left.append(snapshot.balance, style=theme.ACCENT)
    spill = health.get("spill_threshold")
    if not trapi and spill and snapshot.balance == "priority_threshold":
        left.append(" @{:.0%}".format(spill), style=theme.DIM)
    if not trapi and snapshot.rpm_window:
        left.append("  RPM", style=theme.DIM)

    right = Text()
    token = health.get("token") or {}
    if trapi:
        if snapshot.trapi_error:
            right.append("TRAPI status unavailable", style=theme.WARN)
    elif snapshot.error:
        # The proxy is unreachable. Say it here rather than blanking the board:
        # the numbers below are still the last true ones, and their age is in
        # the footer.
        right.append("unreachable", style=theme.CRIT)
    elif health.get("routing") and (not health["routing"].get("ok")
                                   or health["routing"].get("telemetry_dropped")):
        right.append("routing/statistics stale", style=theme.WARN)
    elif token.get("have_token"):
        seconds = token.get("expires_in_seconds") or 0
        right.append("Azure token ", style=theme.DIM)
        right.append("{:.0f}m".format(seconds / 60),
                     style=theme.OK if seconds > 300 else theme.WARN)
    elif health:
        right.append("no token", style=theme.CRIT)
    uptime = health.get("uptime_seconds")
    if uptime is not None:
        right.append("  up {}".format(_duration(uptime)), style=theme.DIM)
    probed = health.get("probed_at")
    if probed:
        right.append("  probed {}".format(probed[:16].replace("T", " ")),
                     style=theme.DIM)

    table.add_row(left, right)
    return table


def _tabs(active: int, snapshot: Snapshot, show_all: bool,
          filter_mode: int = DEFAULT_EVENT_FILTER, kind_mode: int = 0) -> Text:
    out = Text()
    # Shown-of-total where they differ, so the tab does not promise twenty
    # models and then display nine.
    visible_models = len(snapshot.visible_groups(snapshot.models, show_all))
    counts = {"sources": str(len(snapshot.sources)),
              "models": (str(len(snapshot.models))
                         if visible_models == len(snapshot.models)
                         else "{}/{}".format(visible_models,
                                             len(snapshot.models))),
              "events": "{}/{}".format(len(filter_events(snapshot.events, filter_mode, kind_mode)),
                                         len(snapshot.events)),
              "trapi": ("!" if snapshot.trapi_error else
                        "0" if snapshot.trapi.get("enabled") is False else
                        "1" if snapshot.trapi.get("model") else "—")}
    for index, name in enumerate(BOARDS):
        label = " {} {} ".format(BOARD_TITLES[name], counts[name])
        if index == active:
            out.append(label, style="reverse {}".format(theme.ACCENT))
        else:
            out.append(label, style=theme.DIM)
        out.append(" ")
    sessions = (snapshot.affinity or {}).get("live_sessions")
    if sessions:
        out.append("  {} pinned sessions".format(sessions), style=theme.ACCENT)
    return out


def _footer(dash: "Dashboard", snapshot: Snapshot, extent: int) -> Table:
    table = Table.grid(padding=(0, 2), expand=True)
    table.add_column(ratio=1)
    table.add_column(justify="right")

    keys = Text()
    keys.append("←/→", style=theme.ACCENT)
    keys.append(" 看板  ", style=theme.DIM)
    keys.append("↑/↓", style=theme.ACCENT)
    keys.append(" 滚动  ", style=theme.DIM)
    if dash.board == 2:
        keys.append("f", style=theme.ACCENT)
        keys.append(" {}  ".format(EVENT_FILTERS[dash.filter]), style=theme.DIM)
        keys.append("t", style=theme.ACCENT)
        keys.append(" {}  ".format(EVENT_KIND_FILTERS[dash.kind_filter]), style=theme.DIM)
    elif dash.board in (0, 1):
        keys.append("s", style=theme.ACCENT)
        keys.append(" {}  ".format(SORTS[dash.sort]), style=theme.DIM)
        # Never silent. A board that leaves models out has to say how many and
        # which key brings them back, or it reads as a complete picture that
        # happens to be missing the model you came to look for.
        hidden = snapshot.hidden_models()
        keys.append("a", style=theme.ACCENT)
        if dash.show_all:
            keys.append(" 全部旧模型  ", style=theme.DIM)
        elif hidden:
            keys.append(" 隐藏 {} 个旧模型  ".format(hidden), style=theme.DIM)
        else:
            keys.append(" 无旧模型  ", style=theme.DIM)
    keys.append("r", style=theme.ACCENT)
    keys.append(" 刷新  ", style=theme.DIM)
    keys.append("q", style=theme.ACCENT)
    keys.append(" 退出", style=theme.DIM)

    state = Text()
    if extent > 1:
        state.append("{}/{}  ".format(dash.offset[dash.board] + 1, extent),
                     style=theme.DIM)
    is_trapi = BOARDS[dash.board] == "trapi"
    age = snapshot.trapi_age if is_trapi else snapshot.age
    error = snapshot.trapi_error if is_trapi else snapshot.error
    if error:
        state.append(error[:40], style=theme.CRIT)
    elif age is not None:
        state.append("{:.0f}s ago".format(age),
                     style=theme.DIM if age < 5 else theme.WARN)

    if is_trapi:
        note = Text("仅 trapi/ 白名单 · 不参与 Azure 路由", style=theme.LABEL)
        note.append("  /  本地只读 · 计数自启动累计", style=theme.DIM)
        table.add_row(note, state)
    else:
        table.add_row(legend() if dash.board in (0, 1) else keys, state)
    if dash.board in (0, 1) or is_trapi:
        table.add_row(keys, Text(""))
    return table


def _duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 90:
        return "{}s".format(seconds)
    if seconds < 5400:
        return "{}m".format(seconds // 60)
    if seconds < 172800:
        return "{}h".format(seconds // 3600)
    return "{}d".format(seconds // 86400)


# --------------------------------------------------------------------------
# raw input
# --------------------------------------------------------------------------

def _read_keys(timeout: float) -> List[str]:
    """Whatever was typed inside `timeout`, as whole sequences.

    Arrow keys arrive as three or four bytes (ESC [ A, ESC [ 5 ~). Reading one
    byte at a time and dispatching on it would turn every arrow press into an
    escape plus two stray letters, so the whole burst is read and split.
    """
    ready, _, _ = select.select([sys.stdin], [], [], timeout)
    if not ready:
        return []
    try:
        data = os.read(sys.stdin.fileno(), 1024).decode("utf-8", "ignore")
    except OSError:
        return []

    out: List[str] = []
    i = 0
    while i < len(data):
        if data[i] == "\x1b" and i + 1 < len(data) and data[i + 1] == "[":
            j = i + 2
            while j < len(data) and not ("A" <= data[j] <= "Z"
                                         or data[j] == "~"):
                j += 1
            out.append(data[i:j + 1])
            i = j + 1
        else:
            out.append(data[i])
            i += 1
    return out


def run(base_url: str, interval: float = 1.0) -> None:
    """Poll `base_url` and draw until the user quits, or until asked to stop.

    SIGTERM and SIGINT are handled rather than left to the default so that both
    turn into the same orderly exit as pressing `q`. What that buys is the
    `finally` below: a viewer killed from another terminal still puts the tty
    back the way it found it, and a shell left in cbreak mode by a dashboard
    that died on the spot is a more annoying thing to inherit than whatever it
    was showing.

    Nothing here stops the proxy — this process only ever reads.
    """
    poller = Poller(base_url, interval=interval)
    poller.start()
    console = Console()
    dashboard = Dashboard(poller, console)

    fd = sys.stdin.fileno()
    saved = None
    try:
        saved = termios.tcgetattr(fd)
        tty.setcbreak(fd)
    except (termios.error, ValueError):
        saved = None            # not a tty (piped, or under a test): no keys

    previous = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            previous[sig] = signal.signal(
                sig, lambda _s, _f: setattr(dashboard, "quit", True))
        except ValueError:
            pass                # not the main thread; the caller handles it

    try:
        dashboard.run()
    except KeyboardInterrupt:
        pass
    finally:
        for sig, handler in previous.items():
            try:
                signal.signal(sig, handler)
            except ValueError:
                pass
        if saved is not None:
            termios.tcsetattr(fd, termios.TCSADRAIN, saved)
        poller.stop()
