"""The three boards, and the cards they are made of.

Each board answers one question and is laid out for that question rather than
for the shape of the data:

  sources  which resource is being spent, and on what
  models   where a given model can come from, and how much each place offers
  events   what happened, in order, and what the proxy did about it

The first two share a card renderer. They differ only in what a card groups by
and what the row label says, which is the point — if a route looks busy under
one heading it must look equally busy under the other.
"""

from typing import List, Optional

import math
import time

from rich.console import Group as RichGroup
from rich.cells import cell_len
from rich.panel import Panel
from rich.segment import Segment, SegmentLines
from rich.table import Table
from rich.text import Text

from proxy.events import LEVELS, LEVEL_RANK, PROBLEM_KINDS, event_level
from .event_text import kind_label, message as event_message

from . import theme
from .bars import capacity_bar, rpm_capacity, si
from .layout import column_widths, rows, truncate
from .snapshot import Group, RouteView, Snapshot

BOARDS = ("models", "sources", "events", "proxy")
BOARD_TITLES = {"sources": "源", "models": "模型", "events": "事件流", "proxy": "proxy 状态"}

MAX_CARD = 93
CARD_GAP = 2
ROW_GAP = 1
CARD_SIDE_PADDING = 2

BAR_MIN = 8


# --------------------------------------------------------------------------
# cards
# --------------------------------------------------------------------------

def _labels(routes: List[RouteView], kind: str) -> dict:
    """A display label per route key, unique within the card.

    A source card labels rows by model and a model card labels them by source,
    which is the useful thing to read in each — but neither is unique. One
    resource can hold two deployments of the same model (a second SKU is a
    second quota and a genuinely separate destination), so both cards can
    produce two identically-labelled rows that are different places with
    different loads. Where that happens the deployment name is appended, which
    is the part that actually differs.
    """
    base = {}
    for route in routes:
        base[route.key] = ((route.model or route.deployment)
                           if kind == "source" else route.endpoint)

    counts = {}
    for value in base.values():
        counts[value] = counts.get(value, 0) + 1

    labels = {}
    for route in routes:
        value = base[route.key]
        if counts[value] > 1:
            if kind == "source":
                # The deployment name alone. On a source card the base label is
                # already the model, so `gpt-5.1·gpt-5.1` says the same thing
                # twice; the deployment is the whole of what differs between the
                # two rows, and it usually contains the model name anyway.
                value = route.deployment
            else:
                value = "{}·{}".format(value, route.deployment)
        labels[route.key] = value
    return labels


def _route_rows(routes: List[RouteView], width: int, labels: dict,
                pins: dict, detail: bool, show_share: bool = False):
    """Names, pins, our usage, bars, outside usage and ceilings share one row."""
    capacities = {route.key: rpm_capacity(route.capacity_rpm, label=False) for route in routes}
    pin_cells = {key: _pinned_value(value) for key, value in pins.items()}
    ours = {route.key: Text(_usage_number(route.current_rpm), style=theme.OURS) for route in routes}
    others = {route.key: Text(_usage_number(route.other_rpm), style=theme.FOREIGN) for route in routes}
    capacity_width = max([4] + [text.cell_len for text in capacities.values()])
    pin_width = max([2] + [text.cell_len for text in pin_cells.values()])
    ours_width = max([1] + [text.cell_len for text in ours.values()])
    others_width = max([1] + [text.cell_len for text in others.values()])
    shares = {route.key: Text(_share_number(route.share), style=theme.ACCENT)
              for route in routes} if show_share else {}
    share_width = max([1] + [text.cell_len for text in shares.values()]) if show_share else 0
    share_space = share_width + 1 if show_share else 0
    available = max(2, width - pin_width - capacity_width - ours_width - others_width - 5 - share_space)
    bar_min = min(BAR_MIN, available - 1)
    desired_label_width = max([10] + [cell_len(labels.get(route.key, route.key)) for route in routes])
    label_width = max(1, min(desired_label_width, 36, available - bar_min))
    bar_width = available - label_width

    table = Table.grid(padding=(0, 1))
    if show_share:
        table.add_column(width=share_width, justify="right", no_wrap=True)
    table.add_column(width=label_width, no_wrap=True)
    table.add_column(width=pin_width, justify="right", no_wrap=True)
    table.add_column(width=ours_width, justify="right", no_wrap=True)
    table.add_column(width=bar_width, no_wrap=True)
    table.add_column(width=others_width, justify="right", no_wrap=True)
    table.add_column(width=capacity_width, justify="right", no_wrap=True)

    for index, route in enumerate(Snapshot.sorted_routes(routes)):
        if index:
            for _ in range(ROW_GAP):
                table.add_row(*(Text("") for _ in table.columns))
        label = labels.get(route.key, route.key)
        label_style = theme.TEXT if route.busy else theme.LABEL
        table.add_row(*([shares[route.key]] if show_share else []),
                      Text(truncate(label, label_width, middle=not show_share, prefix=show_share), style=label_style),
                      pin_cells.get(route.key, _pinned_value(None)),
                      ours[route.key],
                      capacity_bar(bar_width, route.rpm_load_by_face, route.rpm_other_load),
                      others[route.key],
                      capacities[route.key])
    return table


def _pinned_value(count):
    return Text(str(count) if count else "·",
                style=theme.PINNED, no_wrap=True)


def _card_title(group):
    title = Text(group.name, style=theme.TITLE)
    if group.kind == "source" and group.priority is not None:
        title.append(" p{}".format(group.priority), style=theme.DIM)
    return title


def _card_summary(group, details, pinned):
    maximum = rpm_capacity(group.capacity_rpm if any(
        r.capacity_rpm is not None for r in group.routes) else None)
    summary = Text(no_wrap=True, overflow="ellipsis")
    if group.kind == "model":
        summary.append("route prob.  ", style=theme.ACCENT)
    summary.append("{} pinned".format(pinned if pinned else "·"), style=theme.PINNED)
    if details.plain:
        if summary.plain:
            summary.append(" · ", style=theme.DIM)
        summary.append(details)
    row = Table.grid(padding=(0, 1), expand=True)
    row.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
    row.add_column(width=maximum.cell_len, justify="right", no_wrap=True)
    row.add_row(summary, maximum)
    return row


def _row_pins(routes, snapshot, model=None):
    """Each deployment displays the shared endpoint/model binding count."""
    return {route.key: snapshot.pinned(model or route.model or route.deployment, route.endpoint)
            for route in routes}


def _usage_number(value):
    return (si(value) if abs(value) >= 1000 else "{:.1f}".format(value)).replace(".0", "")


def _share_number(value):
    if value is None:
        return "·"
    number = "{:.2f}".format(value).rstrip("0").rstrip(".")
    return number[1:] if number.startswith("0.") else number


def _source_card(group: Group, width: int, detail: bool, pinned: Optional[int],
                 snapshot: Snapshot, show_all: bool) -> Panel:
    rows_shown = snapshot.visible_routes(group.routes, show_all)

    title = _card_title(group)

    subtitle = Text()
    hidden = len(group.routes) - len(rows_shown)
    if hidden:
        subtitle.append("隐藏 {} 旧".format(hidden), style=theme.DIM)

    body = _route_rows(rows_shown, width - 2 - 2 * CARD_SIDE_PADDING, _labels(rows_shown, "source"),
                       _row_pins(rows_shown, snapshot), detail)
    return Panel(RichGroup(_card_summary(group, subtitle, pinned),
                           *([Text("")] if rows_shown else []), body),
                 title=title, width=width,
                 border_style=theme.severity(
                     _load_or_none(group.peak_rpm_load)),
                 padding=(0, CARD_SIDE_PADDING, 1, CARD_SIDE_PADDING))


def _model_card(group: Group, width: int, detail: bool,
                snapshot: Snapshot) -> Panel:
    title = _card_title(group)

    body = _route_rows(group.routes, width - 2 - 2 * CARD_SIDE_PADDING, _labels(group.routes, "model"),
                       _row_pins(group.routes, snapshot, group.name), detail, show_share=True)
    return Panel(RichGroup(_card_summary(group, Text(), snapshot.pinned(group.name)),
                           *([Text("")] if group.routes else []), body),
                 title=title, width=width,
                 border_style=theme.severity(
                     _load_or_none(group.peak_rpm_load)),
                 padding=(0, CARD_SIDE_PADDING, 1, CARD_SIDE_PADDING))


def _load_or_none(value: float) -> Optional[float]:
    return None if value < 0 else value


# --------------------------------------------------------------------------
# boards
# --------------------------------------------------------------------------

def _grid(cards, widths, gap: int = CARD_GAP) -> Table:
    """Lay one row of cards out at the supplied widths, aligned left."""
    table = Table.grid(padding=(0, 0))
    for i, w in enumerate(widths):
        if i:
            table.add_column(width=gap)
        table.add_column(width=w)
    cells = []
    for i, card in enumerate(cards):
        if i:
            cells.append("")
        cells.append(card)
    table.add_row(*cells)
    return table


class _SourceColumns:
    """Pack measured source cards into balanced, independently flowing columns."""

    def __init__(self, groups, widths, detail, snapshot, show_all):
        self.groups, self.widths, self.detail = groups, widths, detail
        self.snapshot, self.show_all = snapshot, show_all

    def __rich_console__(self, console, options):
        widths = self.widths
        pins = self.snapshot.affinity.get("sessions_per_endpoint") or {}

        def render_card(group, width):
            count = pins.get(group.name, 0 if "sessions_per_endpoint" in self.snapshot.affinity else None)
            card = _source_card(group, width, self.detail, count, self.snapshot, self.show_all)
            return console.render_lines(card, options.update(width=width, height=None))

        measured = [(group, render_card(group, min(widths))) for group in self.groups]
        measured.sort(key=lambda item: -len(item[1]))
        columns = [[] for _ in widths]
        for group, lines in measured:
            column = min(range(len(widths)), key=lambda i: len(columns[i]))
            width = widths[column]
            if width != min(widths):
                lines = render_card(group, width)
            if columns[column]:
                columns[column].append([Segment(" " * width)])
            columns[column].extend(lines)
        yield _grid([SegmentLines(lines, new_lines=True) for lines in columns], widths)


def _card_widths(width, snapshot, show_all):
    """Add columns as needed to fill the terminal within the card width cap."""
    reference = snapshot.sources or snapshot.visible_groups(snapshot.models, show_all)
    if not reference or width <= 0:
        return []
    columns = min(len(reference), math.ceil((width + CARD_GAP) / (MAX_CARD + CARD_GAP)))
    grid_width = min(width, columns * MAX_CARD + (columns - 1) * CARD_GAP)
    return column_widths(grid_width, columns, min_card=1, gap=CARD_GAP)


def render_groups(groups: List[Group], width: int, height: int, offset: int,
                  sort: str, snapshot: Snapshot, kind: str,
                  show_all: bool = False):
    """A scrollable grid of cards. Returns (renderable, total card rows)."""
    ordered = snapshot.sorted_groups(
        snapshot.visible_groups(groups, show_all), sort)
    if not ordered:
        return Text("no routes — has the probe been run?", style=theme.DIM), 0

    widths = _card_widths(width, snapshot, show_all)
    per_row = len(widths)
    if kind == "source":
        return _SourceColumns(ordered, widths, False, snapshot, show_all), len(ordered)

    cards = []
    for index, group in enumerate(ordered):
        card_width = widths[index % per_row]
        cards.append(_model_card(group, card_width, False, snapshot))

    banded = rows(cards, per_row)
    visible = banded[offset:]
    return (RichGroup(*[_grid(band, widths[:len(band)]) for band in visible]),
            len(banded))


# --------------------------------------------------------------------------
# events
# --------------------------------------------------------------------------

EVENT_FILTERS = ("DEBUG+", "INFO+", "WARNING+", "ERROR")
DEFAULT_EVENT_FILTER = LEVEL_RANK["info"]
EVENT_KIND_FILTERS = ("全部类型", "问题类型", "他人流量")


def filter_events(events: List[dict], mode: int, kind_mode: int = 0) -> List[dict]:
    threshold = LEVEL_RANK[LEVELS[mode]]
    events = [e for e in events if LEVEL_RANK[event_level(
        e.get("kind", ""), e.get("level", "info"), e)] >= threshold]
    if kind_mode == 1:
        return [e for e in events if e.get("kind") in PROBLEM_KINDS]
    if kind_mode == 2:
        # The only events that carry a revised estimate of what other tenants
        # hold. `foreign` events always do; a throttle does when it moved one.
        return [e for e in events
                if e.get("kind") == "foreign" or e.get("foreign_updated")]
    return events


def render_events(events: List[dict], width: int, height: int, offset: int,
                  filter_mode: int, dropped: bool, kind_mode: int = 0):
    """Newest first. Returns (renderable, total lines available).

    Newest first rather than a tailing log, because this is read to answer
    "what just went wrong", and a stream that scrolls under the cursor cannot
    be read at all. Nothing auto-follows; the top is always now.

    Request direction and state changes share a bounded context column. The route
    already identifies the deployment; events without a route fall back to the
    requested model. Column widths stay stable while scrolling, and extra room
    on wide terminals goes to the trailing explanation.
    """
    shown = list(reversed(filter_events(events, filter_mode, kind_mode)))

    if not shown and not dropped:
        return Text("没有匹配事件", style=theme.DIM), 0

    level_width = 5 if width >= 90 else 1
    # Keep the explanation close to the route, including on wide terminals.
    fixed_width = 8 + 1 + level_width + 10 + 5
    message_width = max(12, min(48, width // 4))
    context_width = min(52, max(1, width - fixed_width - message_width))

    table = Table.grid(padding=(0, 1), expand=True)
    table.add_column(width=8, no_wrap=True)              # time
    table.add_column(width=1, no_wrap=True)              # mark
    table.add_column(width=level_width, no_wrap=True)    # severity
    table.add_column(width=10, no_wrap=True)             # kind
    table.add_column(width=context_width, no_wrap=True)  # direction / state / route
    table.add_column(ratio=1, no_wrap=True)              # what happened

    for event in shown[offset:offset + max(1, height)]:
        kind = event.get("kind", "?")
        mark, _ = theme.EVENT_STYLE.get(kind, ("·", theme.DIM))
        level = event_level(kind, event.get("level", "info"), event)
        colour = _level_style(level)
        label = "WARN" if level == "warning" else level.upper()
        stamp = time.strftime("%H:%M:%S", time.localtime(event.get("at", 0)))
        table.add_row(Text(stamp, style=theme.DIM),
            Text(mark, style=colour),
            Text(label if level_width == 5 else label[0], style=colour),
            Text(kind_label(event), style=colour),
            _event_context(event, context_width),
            _event_message(event))
    return table, len(shown)


def _level_style(level: str) -> str:
    return {"debug": theme.DIM, "info": theme.LABEL,
            "warning": theme.WARN, "error": theme.CRIT}[level]


def _event_message(event: dict) -> Text:
    """What happened, in the proxy's own words."""
    level = event_level(event.get("kind", ""), event.get("level", "info"), event)
    style = _level_style(level)
    return Text(event_message(event), style=style,
                overflow="ellipsis", no_wrap=True)


def _event_context(event: dict, width: int) -> Text:
    """Show the destination once, or the current route with its revised state."""
    out = Text(no_wrap=True, overflow="ellipsis")
    target = event.get("to_route")
    if target:
        out.append("↻ " if target == event.get("route") else "→ ", style=theme.DIM)
        out.append(target, style=theme.ACCENT)
        return out

    route = event.get("route") or event.get("endpoint") or event.get("model") or ""
    out.append(route, style=theme.TEXT)
    change = _event_change(event, width)
    if change.plain:
        if route:
            out.append(" · ", style=theme.DIM)
        out.append(change)
    return out


def _event_change(event: dict, width: int) -> Text:
    """What the proxy now believes differently, if anything.

    The foreign clause is the reason this board exists. It is the only moment
    the proxy revises its belief about how much of a deployment belongs to
    someone else — learnable, as the AIMD note in proxy/server.py explains, at
    no other time than a throttle — and in the log it lands on a separate line
    several lines away from the demotion that caused it.
    """
    out = Text(no_wrap=True, overflow="crop")
    if not width:
        return out

    if event.get("foreign_updated") or event.get("kind") == "foreign":
        other_rpm = event.get("other_rpm")
        capacity_rpm = event.get("capacity_rpm")
        if other_rpm is not None and capacity_rpm:
            out.append("外部 ", style=theme.DIM)
            out.append("{:.1f} RPM".format(other_rpm), style=theme.FOREIGN)
            out.append(" / 最大 {:.1f}".format(capacity_rpm), style=theme.DIM)
            return out
        before, after = event.get("foreign_before"), event.get("foreign_after")
        if before is not None and after is not None:
            out.append("外部 ", style=theme.DIM)
            arrow = "→" if after > before else "="
            out.append("{:.0f}%{}{:.0f}%".format(before * 100, arrow,
                                                 after * 100),
                       style=theme.FOREIGN)
            ours = event.get("our_load")
            if ours is not None:
                out.append("（本机 {:.0f}%）".format(ours * 100), style=theme.DIM)
            return out

    to_route = event.get("to_route")
    if to_route:
        if to_route == event.get("route"):
            out.append("同一部署", style=theme.ACCENT)
            return out
        out.append("→ ", style=theme.DIM)
        out.append(truncate(to_route, width - 2), style=theme.ACCENT)
        return out

    # Deliberately nothing for an ordinary response. Its duration is already at
    # the end of the message, and repeating it here made every successful line
    # say the same number twice while the column exists to say what CHANGED.
    return out
