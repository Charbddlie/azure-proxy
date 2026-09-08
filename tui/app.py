"""Dashboard with line scrolling, clickable tabs, and responsive input.

The poller fetches data once a second. Idle refreshes keep ages current at 4 Hz;
input requests an immediate frame, coalesced at up to 60 Hz. Rendered card lines
are cached between data/layout changes so scrolling only slices the viewport.
"""

import os
import json
import re
import select
import signal
import sys
import termios
import time
import tty
from contextlib import contextmanager
from collections import OrderedDict
from typing import List

from rich.console import Console, Group as RichGroup
from rich.live import Live
from rich.rule import Rule
from rich.segment import Segment, SegmentLines
from rich.style import Style
from rich.table import Table
from rich.text import Text

from . import theme
from .bars import legend
from .boards import (BOARD_TITLES, BOARDS, DEFAULT_EVENT_FILTER, EVENT_FILTERS,
                     EVENT_KIND_FILTERS, filter_events, render_events, render_groups)
from .client import Poller
from .snapshot import Snapshot

REDRAW_HZ = 4.0
INPUT_HZ = 60.0
DEFAULT_SCROLL_LINES = 2
EVENT_ROW_CACHE_SIZE = 1024
MOUSE_EVENT = re.compile(r"\x1b\[<(\d+);(\d+);(\d+)([Mm])")


class Dashboard:

    def __init__(self, poller: Poller, console: Console, scroll_lines: int = DEFAULT_SCROLL_LINES):
        if isinstance(scroll_lines, bool) or not isinstance(scroll_lines, int) or scroll_lines < 1:
            raise ValueError("scroll_lines must be a positive integer")
        self.poller = poller
        self.console = console
        self.scroll_lines = scroll_lines
        self.board = 0
        self.offset = [0] * len(BOARDS)  # one scroll position per board
        self.filter = DEFAULT_EVENT_FILTER
        self.kind_filter = 0
        self.show_all = False
        self.quit = False
        self._extent = [0] * len(BOARDS)  # valid viewport positions, from the last render
        self._body_height = 1
        self._tab_regions = []         # (row, first column, exclusive end, board), 1-based
        self._button_regions = []
        self._card_cache = {}
        self._event_rows = OrderedDict()
        self._can_acknowledge = False

    # -- input ------------------------------------------------------------
    def key(self, seq: str) -> None:
        mouse = MOUSE_EVENT.fullmatch(seq)
        if mouse:
            button, x, y = map(int, mouse.groups()[:3])
            if mouse.group(4) == "M":
                if button & ~28 == 64:         # wheel up, including modifiers
                    self._scroll(-self.scroll_lines)
                elif button & ~28 == 65:
                    self._scroll(self.scroll_lines)
                elif button == 0:             # left press; releases/drags do nothing
                    for row, start, end, board in self._tab_regions:
                        if y == row and start <= x < end:
                            self.board = board
                            break
                    for row, start, end, action in self._button_regions:
                        if y == row and start <= x < end:
                            self._activate_button(action)
                            break
        elif seq == "\x03":
            self.quit = True

    def _activate_button(self, action: str) -> None:
        if action == "cycle_level" and self.board == 2:
            self.filter = (self.filter + 1) % len(EVENT_FILTERS)
            self.offset[2] = 0
        elif action == "cycle_kind" and self.board == 2:
            self.kind_filter = (self.kind_filter + 1) % len(EVENT_KIND_FILTERS)
            self.offset[2] = 0
        elif action == "toggle_legacy" and self.board in (0, 1):
            self.show_all = not self.show_all
            # The list just got longer or shorter under the cursor. Keeping the
            # old offset would leave it pointing at a row that is no longer
            # there, which reads as the board having jumped on its own.
            self.offset[0] = self.offset[1] = 0
        elif action == "acknowledge_gap" and self.board == 2 and self._can_acknowledge:
            self.poller.acknowledge_gap()
            self._can_acknowledge = False

    def _scroll(self, delta: int) -> None:
        limit = max(0, self._extent[self.board] - 1)
        self.offset[self.board] = max(
            0, min(limit, self.offset[self.board] + delta))

    # -- rendering --------------------------------------------------------
    def render(self):
        raw = self.poller.snapshot()
        snapshot = Snapshot(raw)
        self._can_acknowledge = bool(snapshot.gap)
        width = self.console.width
        header = RichGroup(_top_bar(snapshot, width, getattr(self.poller, "base_url", None)),
                           Text(""),
                           _tabs(self.board, snapshot, self.show_all, self.filter,
                                 self.kind_filter), Rule(style=theme.BORDER))
        footer = _footer(self, snapshot, self._extent[self.board])
        options = self.console.options.update(height=None)
        header_lines = self.console.render_lines(header, options)
        self._tab_regions = _hit_regions(header_lines, "tab")
        header_height = len(header_lines)
        footer_height = len(self.console.render_lines(footer, options, pad=False))
        chrome_height = header_height + footer_height
        body_height = max(1, self.console.height - chrome_height)

        def render_body(height):
            if self.board == 2:
                shown = list(reversed(filter_events(snapshot.events, self.filter, self.kind_filter)))
                count = len(shown)
                extent = max(0, count - height) + 1 if count else 0
                self.offset[2] = min(self.offset[2], max(0, extent - 1))
                if not shown:
                    body, _ = render_events([], width, height, 0, self.filter,
                                            snapshot.dropped, self.kind_filter)
                    return self.console.render_lines(body, options.update(height=height)), extent
                lines = []
                for event in shown[self.offset[2]:self.offset[2] + height]:
                    key = (width, json.dumps(event, sort_keys=True, ensure_ascii=False))
                    if key not in self._event_rows:
                        row, _ = render_events([event], width, 1, 0, 0, False)
                        self._event_rows[key] = self.console.render_lines(row, options.update(height=1))[0]
                        if len(self._event_rows) > EVENT_ROW_CACHE_SIZE:
                            self._event_rows.popitem(last=False)
                    self._event_rows.move_to_end(key)
                    lines.append(self._event_rows[key])
                lines += [[Segment(" " * width)]] * (height - len(lines))
                return lines, extent

            if self.board == 3:
                lines = self.console.render_lines(_proxy_status(snapshot), options)
                return self._viewport(lines, height, width)

            # Poller replaces published route documents. Keep their rendered
            # lines until the data or layout changes, independently per board.
            cache_key = (raw.get("routes"), snapshot.affinity, width, height,
                         self.show_all)
            cached = self._card_cache.get(self.board)
            if cached is None or cached[0] != cache_key:
                body, _ = render_groups(
                    snapshot.models if self.board == 1 else snapshot.sources,
                    width, height, 0, "activity", snapshot,
                    "model" if self.board == 1 else "source", self.show_all)
                lines = self.console.render_lines(body, options)
                self._card_cache[self.board] = (cache_key, lines)
            else:
                lines = cached[1]
            return self._viewport(lines, height, width)

        body, extent = render_body(body_height)
        footer = _footer(self, snapshot, extent)
        extra = len(self.console.render_lines(footer, options, pad=False)) - footer_height
        if extra > 0:
            # A newly visible scroll counter can wrap the footer on narrow terminals.
            body_height = max(1, body_height - extra)
            body, extent = render_body(body_height)
            footer = _footer(self, snapshot, extent)
        self._extent[self.board] = extent
        self._body_height = body_height

        footer_lines = self.console.render_lines(footer, options)
        self._button_regions = _hit_regions(
            footer_lines[:max(0, self.console.height - header_height - body_height)],
            "action", header_height + body_height + 1)
        return SegmentLines((header_lines + body + footer_lines)[:self.console.height],
                            new_lines=True)

    def _viewport(self, lines, height, width):
        extent = max(0, len(lines) - height) + 1
        self.offset[self.board] = min(self.offset[self.board], extent - 1)
        start = self.offset[self.board]
        visible = lines[start:start + height]
        visible += [[Segment(" " * width)]] * (height - len(visible))
        return visible, extent

    # -- loop -------------------------------------------------------------
    def run(self) -> None:
        reader = _InputReader()
        with _mouse_tracking(self.console), Live(
                self.render(), console=self.console, screen=True,
                auto_refresh=False, transient=False) as live:
            next_refresh = time.monotonic() + 1.0 / REDRAW_HZ
            next_frame = 0.0
            dirty = False
            while not self.quit:
                now = time.monotonic()
                deadline = next_frame if dirty else next_refresh
                keys = reader.read(max(0.0, deadline - now))
                for seq in keys:
                    self.key(seq)
                dirty = dirty or bool(keys)
                now = time.monotonic()
                if self.quit:
                    break
                if (dirty or now >= next_refresh) and now >= next_frame:
                    next_frame = now + 1.0 / INPUT_HZ
                    live.update(self.render(), refresh=True)
                    next_refresh = time.monotonic() + 1.0 / REDRAW_HZ
                    dirty = False


# --------------------------------------------------------------------------
# chrome
# --------------------------------------------------------------------------

def _hit_regions(lines, metadata, first_row=1):
    regions = []
    for y, line in enumerate(lines, first_row):
        x = 1
        for segment in line:
            end = x + segment.cell_length
            value = segment.style.meta.get(metadata) if segment.style else None
            if value is not None:
                regions.append((y, x, end, value))
            x = end
    return regions


def _freshness(snapshot: Snapshot) -> Text:
    out = Text(no_wrap=True, overflow="ellipsis")
    age = snapshot.age
    out.append("fetch {}".format(_duration(age) + " ago" if age is not None else "—"),
               style=theme.CRIT if snapshot.error else theme.WARN if age is not None and age >= 5 else theme.DIM)
    out.append("  stats {}".format(_duration(snapshot.stats_age) + " ago"
                                  if snapshot.stats_age is not None else "—"),
               style=theme.WARN if snapshot.stats_stale else theme.DIM)
    if snapshot.stats_stale:
        out.append("  stale", style=theme.WARN)
    return out


def _top_bar(snapshot: Snapshot, width=120, base_url=None):
    brand = Text("azure-proxy", style="bold " + theme.TITLE, no_wrap=True, overflow="ellipsis")
    if not isinstance(base_url, str):
        host, port = snapshot.health.get("host"), snapshot.health.get("port")
        base_url = "{}:{}".format(host, port) if host else ""
    if base_url:
        brand.append("  " + base_url.split("://", 1)[-1], style=theme.LABEL)
    inline_status = brand.cell_len + 18 <= width - 28
    if inline_status:
        brand.append("  ")
        brand.append(_status_line(snapshot))
    table = Table.grid(padding=(0, 2), expand=True)
    table.add_column(width=min(brand.cell_len, max(11, width - 28)), no_wrap=True)
    table.add_column(ratio=1, justify="right", no_wrap=True)
    table.add_row(brand, _freshness(snapshot))
    return table if inline_status else RichGroup(table, _status_line(snapshot))

def _header(snapshot: Snapshot) -> Table:
    health = snapshot.health
    table = Table.grid(padding=(0, 2), expand=True)
    table.add_column(ratio=1)
    table.add_column(justify="right")

    left = Text()
    left.append("azure-proxy", style="bold {}".format(theme.TITLE))
    host, port = health.get("host"), health.get("port")
    if host:
        left.append("  {}:{}".format(host, port), style=theme.LABEL)
    if snapshot.balance:
        left.append("  balance ", style=theme.DIM)
        left.append(snapshot.balance, style=theme.ACCENT)
    spill = health.get("spill_threshold")
    if spill and snapshot.balance == "priority_threshold":
        left.append(" @{:.0%}".format(spill), style=theme.DIM)
    if snapshot.rpm_window:
        left.append("  RPM", style=theme.DIM)

    right = Text()
    token = health.get("token") or {}
    if snapshot.health_error:
        right.append("last known credentials", style=theme.DIM)
    elif token.get("have_token"):
        seconds = token.get("expires_in_seconds") or 0
        right.append("token ", style=theme.DIM)
        right.append("{:.0f}m".format(seconds / 60),
                     style=theme.OK if seconds > 300 else theme.WARN)
    elif health:
        right.append("no token", style=theme.CRIT)
    probed = health.get("probed_at")
    if probed:
        right.append("  probed {}".format(probed[:16].replace("T", " ")),
                     style=theme.DIM)

    table.add_row(left, right)
    return table


def _process_states(snapshot: Snapshot):
    """One status policy for both the compact indicators and detailed view."""
    if snapshot.health_error:
        status, style = "unreachable", theme.CRIT
    elif not snapshot.health:
        status, style = "connecting", theme.DIM
    elif snapshot.health_age is not None and snapshot.health_age >= 5:
        status, style = "unconfirmed", theme.WARN
    else:
        status, style = "online", theme.OK
    serving_state = (status, style)
    state = snapshot.routing
    if (status != "online" and not snapshot.health.get("local_status")) or not state:
        route_status, style = "unknown", theme.WARN
    elif state.get("ready") is False:
        route_status, style = "stopped", theme.CRIT
    elif snapshot.heartbeat_age is not None and snapshot.heartbeat_age >= 3:
        route_status, style = "no heartbeat", theme.CRIT
    elif state.get("exchange_error") or not state.get("ok"):
        route_status, style = "degraded", theme.WARN
    else:
        route_status, style = "online", theme.OK
    return serving_state, (route_status, style)


def _status_line(snapshot: Snapshot) -> Text:
    serving, routing = _process_states(snapshot)
    out = Text(no_wrap=True, overflow="ellipsis")
    out.append("serving", style=serving[1])
    out.append("  ")
    out.append("routing", style=routing[1])
    return out


def _proxy_status(snapshot: Snapshot):
    affinity = snapshot.affinity
    window = affinity.get("active_window_seconds")
    label = "活跃会话绑定" if window is not None else "会话绑定统计"
    pins = Text("{}  {} pinned".format(label, affinity.get("live_sessions", "?")),
                style=theme.LABEL)
    if window is not None:
        duration = "{:g} 分钟".format(window / 60) if window >= 60 else "{:g} 秒".format(window)
        pins.append("  近 {}，含进行中的请求".format(duration), style=theme.DIM)
    if not affinity.get("model_tracking"):
        pins.append("  源 × 模型明细等待 serving 升级", style=theme.DIM)
    elif affinity.get("unattributed_sessions"):
        pins.append("  {} 个既有会话的模型明细随后续请求补齐".format(
            affinity["unattributed_sessions"]), style=theme.DIM)
    errors = [Text("取数异常  " + snapshot.error, style=theme.CRIT)] if snapshot.error else []
    return RichGroup(_header(snapshot), _processes(snapshot), pins, *errors, _event_notice(snapshot))


def _processes(snapshot: Snapshot) -> RichGroup:
    """HTTP proves serving reachability; routing liveness comes from its heartbeat."""
    (status, serving_style), (route_status, routing_style) = _process_states(snapshot)
    serving = Text("serving ", style=theme.LABEL, no_wrap=True, overflow="ellipsis")
    serving.append(status, style=serving_style)
    pid = snapshot.health.get("pid")
    if pid is not None:
        serving.append("  {}PID {}".format("" if status == "online" else "last ", pid),
                       style=theme.DIM)
    uptime = snapshot.health.get("uptime_seconds")
    if uptime is not None:
        serving.append("  up {}".format(_duration(uptime)), style=theme.DIM)

    state = snapshot.routing
    routing = Text("routing ", style=theme.LABEL, no_wrap=True, overflow="ellipsis")
    routing.append(route_status, style=routing_style)
    pid = state.get("pid")
    if pid is not None:
        routing.append("  {}PID {}".format(
            "" if route_status in ("online", "degraded") else "last ", pid), style=theme.DIM)
    if snapshot.heartbeat_age is not None:
        routing.append("  heartbeat {} ago".format(_duration(snapshot.heartbeat_age)),
                       style=theme.DIM if route_status == "online" else theme.WARN)
    def count(name):
        value = state.get(name)
        return "{:,}".format(value) if isinstance(value, int) else "?"
    telemetry = Text("统计记录  待处理 {}  待写入 {}  已丢失 {}".format(
        count("backlog"), count("telemetry_pending"), count("telemetry_dropped")),
        style=theme.WARN if state.get("backlog") or state.get("telemetry_dropped") else theme.DIM,
        no_wrap=True, overflow="ellipsis")
    supervisor = snapshot.health.get("supervisor") or {}
    owner = Text(no_wrap=True, overflow="ellipsis")
    if supervisor:
        owner.append("管理进程  {}  PID {}".format(
            "在线" if supervisor.get("ok") and (status == "online" or snapshot.health.get("local_status")) else "状态未知",
            supervisor.get("pid", "?")), style=theme.DIM)
        workers = Text("接流进程  PID {}  等待旧请求结束的进程 {}".format(supervisor.get("active", "?"),
            ",".join(map(str, supervisor.get("draining", []))) or "0"),
            style=theme.DIM, no_wrap=True, overflow="ellipsis")
        if supervisor.get("starting"):
            workers.append("  预热进程 PID {}".format(supervisor["starting"]), style=theme.WARN)
    storage = snapshot.health.get("affinity_store") or {}
    persistence = Text(no_wrap=True, overflow="ellipsis")
    if storage:
        persistence.append("会话绑定  {}  待写入 {}".format(
            "持久化正常" if storage.get("ok") else "持久化异常", storage.get("pending", "?")),
            style=theme.OK if storage.get("ok") else theme.CRIT)
    return RichGroup(serving, routing, telemetry, *([owner, workers] if supervisor else []),
                     *([persistence] if storage else []))


def _event_notice(snapshot):
    if snapshot.gap:
        labels = {"buffer_overwrite": "缓冲覆盖", "limit": "返回限额截断",
                  "retention": "保留期清理", "legacy_unknown": "原因未知"}
        count = snapshot.gap.get("count")
        return Text("轮询期间漏读 {} 条事件：{}".format(
            count if count is not None else "未知数量",
            "、".join(labels.get(r, r) for r in snapshot.gap.get("reasons", []))), style=theme.WARN)
    if snapshot.history_notice:
        return Text(snapshot.history_notice, style=theme.DIM)
    return Text("", end="")


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
                                         len(snapshot.events)), "proxy": ""}
    for index, name in enumerate(BOARDS):
        label = " {}{} ".format(BOARD_TITLES[name], " " + counts[name] if counts[name] else "")
        if index == active:
            style = Style(color=theme.ACCENT, reverse=True, meta={"tab": index})
        else:
            style = Style(color=theme.DIM, meta={"tab": index})
        out.append(label, style=style)
        out.append(" ")
    return out


def _footer(dash: "Dashboard", snapshot: Snapshot, extent: int):
    keys = Text(no_wrap=True, overflow="ellipsis")

    def button(label, action):
        if keys.plain:
            keys.append(" ")
        keys.append("[{}]".format(label), style=Style(
            color=theme.TEXT, bgcolor=theme.BORDER, meta={"action": action}))

    if dash.board == 2:
        button("等级: " + EVENT_FILTERS[dash.filter], "cycle_level")
        button("类型: " + EVENT_KIND_FILTERS[dash.kind_filter], "cycle_kind")
    elif dash.board in (0, 1):
        hidden = snapshot.hidden_models()
        if dash.show_all:
            button("收起旧模型", "toggle_legacy")
        else:
            button("显示旧模型 {}".format(hidden), "toggle_legacy")
    if dash.board == 2 and snapshot.gap:
        button("确认提示", "acknowledge_gap")

    state = Text(no_wrap=True, overflow="ellipsis")
    if extent > 1:
        state.append("{}/{}  ".format(dash.offset[dash.board] + 1, extent),
                     style=theme.DIM)
    if snapshot.missed_events or snapshot.unknown_gaps:
        state.append("  漏读累计 {}{}".format(snapshot.missed_events,
            " + {} 次数量未知".format(snapshot.unknown_gaps) if snapshot.unknown_gaps else ""), style=theme.WARN)

    explanation = Text(no_wrap=True, overflow="ellipsis")
    if state.plain:
        explanation.append(state)
    if dash.board in (0, 1):
        if explanation.plain:
            explanation.append("  ")
        explanation.append(legend())
    width = dash.console.width
    keys.truncate(width, overflow="ellipsis")
    gap = 2 if keys.plain and explanation.plain else 0
    explanation.truncate(max(0, width - keys.cell_len - gap), overflow="ellipsis")
    line = Text(no_wrap=True, overflow="ellipsis")
    line.append(keys)
    line.append(" " * max(0, width - keys.cell_len - explanation.cell_len))
    line.append(explanation)
    return RichGroup(line, Text(""))


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

class _InputReader:
    """Preserve partial terminal sequences across reads, including SGR mouse."""

    def __init__(self):
        self.buffer = ""

    def read(self, timeout: float) -> List[str]:
        ready, _, _ = select.select([sys.stdin], [], [], timeout)
        if not ready:
            return []
        try:
            data = os.read(sys.stdin.fileno(), 4096)
        except OSError:
            return []
        return self.feed(data.decode("utf-8", "ignore")) if data else ["\x03"]

    def feed(self, data: str) -> List[str]:
        self.buffer += data
        out = []
        while self.buffer:
            if self.buffer[0] != "\x1b":
                out.append(self.buffer[0])
                self.buffer = self.buffer[1:]
                continue
            if len(self.buffer) == 1:
                break
            if self.buffer[1] not in ("[", "O"):
                self.buffer = self.buffer[1:]
                continue
            end = 2
            while end < len(self.buffer) and " " <= self.buffer[end] <= "?":
                end += 1
            if end == len(self.buffer):
                if end > 64:
                    self.buffer = ""  # bound malformed/incomplete reports
                break
            if "@" <= self.buffer[end] <= "~":
                out.append(self.buffer[:end + 1])
                self.buffer = self.buffer[end + 1:]
            else:
                self.buffer = self.buffer[end:]
        return out


@contextmanager
def _mouse_tracking(console: Console):
    """Enable click/wheel reports while the TUI owns the terminal."""
    enabled = console.is_terminal and not console.is_dumb_terminal and sys.stdin.isatty()
    stream = console.file
    try:
        if enabled:
            stream.write("\x1b[?1000h\x1b[?1006h")
            stream.flush()
        yield
    finally:
        if enabled:
            stream.write("\x1b[?1000l\x1b[?1006l")
            stream.flush()


def run(base_url: str, interval: float = 1.0, local_root=None,
        scroll_lines: int = DEFAULT_SCROLL_LINES) -> None:
    """Poll `base_url` and draw until the user quits, or until asked to stop.

    SIGTERM and SIGINT are handled rather than left to the default so that both
    turn into an orderly exit. What that buys is the
    `finally` below: a viewer killed from another terminal still puts the tty
    back the way it found it, and a shell left in cbreak mode by a dashboard
    that died on the spot is a more annoying thing to inherit than whatever it
    was showing.

    Nothing here stops the proxy — this process only ever reads.
    """
    poller = Poller(base_url, interval=interval, local_root=local_root)
    console = Console()
    dashboard = Dashboard(poller, console, scroll_lines=scroll_lines)
    poller.start()

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
