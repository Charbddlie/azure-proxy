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

import time

from rich.console import Group as RichGroup
from rich.cells import cell_len
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from . import theme
from .bars import capacity_bar, percent, si
from .layout import column_widths, rows, truncate
from .snapshot import Group, RouteView, Snapshot

BOARDS = ("sources", "models", "events")
BOARD_TITLES = {"sources": "源", "models": "模型", "events": "事件流"}

# Below this a card cannot hold a name, a bar and a percentage on one line, so
# the grid stops adding columns and lets the cards get wider instead.
MIN_CARD = 46

# How much of a card's inner width the bar may take. The rest is the row label
# and the percentage, both of which have a floor — a bar that grew until the
# name beside it was three letters would be a very precise picture of something
# unidentifiable.
BAR_SHARE = 0.45
BAR_MIN = 8


def _bar_width(inner: int, label_width: int) -> int:
    spare = inner - label_width - 6          # 6 = gap + " 100%"
    return max(BAR_MIN, min(int(inner * BAR_SHARE), spare))


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
                suffix: dict, detail: bool):
    """The body of a card: one row per route, plus a detail line if it fits.

    Takes the rows to draw rather than the group they came from, because the
    two cards choose their rows differently — a source card drops idle legacy
    deployments, a model card shows every source it has. Passing the group
    would put that choice in here, twice.

    The bar row is a three-column grid; the detail line is NOT part of it. A
    long cell in a `Table.grid` grows its column and pushes the fixed-width
    ones off the end — the percentage column vanished entirely the first time
    this was one table — so the wide line gets its own full-width row and is
    truncated to the card by hand.

    `detail` is dropped first when the terminal gets short, before anything is
    truncated. The bar and the percentage are the card; the second line is
    commentary on them.
    """
    label_width = max(10, min(22, width - BAR_MIN - 8))
    bar_width = _bar_width(width, label_width)

    lines = []
    for route in Snapshot.sorted_routes(routes):
        label = labels.get(route.key, route.key)
        tail = suffix.get(route.key, "")
        if tail:
            label = "{} {}".format(label, tail)
        row = Table.grid(padding=(0, 1), expand=True)
        row.add_column(width=label_width, no_wrap=True)
        row.add_column(width=bar_width, no_wrap=True)
        row.add_column(justify="right", width=4, no_wrap=True)
        row.add_row(Text(truncate(label, label_width),
                         style=theme.TEXT if route.busy else theme.LABEL),
                    capacity_bar(bar_width, route.by_face,
                                 route.foreign_load),
                    percent(route.total_load))
        lines.append(row)
        if detail:
            # Indented to start where the bar starts, so the numbers sit under
            # the picture they describe rather than under the name.
            lines.append(_detail(route, width, label_width + 1))
    return RichGroup(*lines)


def _detail(route: RouteView, width: int, indent: int) -> Text:
    """The line under a bar: what the three claims actually are, in numbers.

    `ours` and `others` are printed even when they are zero, because their
    absence is what the reader is checking for — "is anyone else on this" has
    a different answer from "we have not measured this yet", and only a printed
    zero distinguishes them.

    Built by appending clauses only while they fit. Truncating the finished
    line would cut it mid-number, and a number missing its last digit is worse
    than a clause that is not there at all.
    """
    indent = max(0, min(indent, width - 12))
    out = Text(" " * indent, style=theme.DIM, no_wrap=True)
    budget = width - indent

    def room(text: str) -> bool:
        # Cells, not characters: the fixed clauses below are Chinese, which is
        # two columns per glyph, and measuring them with len() would let the
        # line run a clause past the edge of the card.
        return cell_len(out.plain) - indent + cell_len(text) <= budget

    if route.total_load is None:
        out.append("尚未测得上限", style=theme.DIM)
        granted = route.data.get("capacity_tokens")
        if granted and room(" · granted 000k TPM"):
            out.append(" · 授权 {}".format(si(granted, " TPM")),
                       style=theme.DIM)
        return out

    out.append("ours ", style=theme.DIM)
    out.append("{:.0f}%".format((route.our_load or 0.0) * 100), style=theme.OURS)
    out.append(" · others ", style=theme.DIM)
    out.append("{:.0f}%".format(route.foreign_load * 100), style=theme.FOREIGN)

    age = route.data.get("foreign_observed_age_seconds")
    if route.foreign_load > 0 and age is not None and room(" (00s)"):
        # An estimate reclaimed linearly since the last throttle: how old it is
        # decides how much of it to believe.
        out.append(" ({:.0f}s)".format(age), style=theme.DIM)

    sent = route.data.get("sent_requests_in_window") or 0
    tokens = si(route.data.get("sent_tokens_in_window") or 0)
    clause = " · {} req {}".format(sent, tokens)
    if room(clause):
        out.append(clause, style=theme.DIM)
        dimension = route.data.get("load_dimension")
        unit = "TPM" if dimension == "tokens" else "RPM"
        if dimension and room("/" + unit):
            out.append("/{}".format(unit), style=theme.DIM)

    if route.parked > 0 and room(" parked 00s"):
        out.append("  parked {:.0f}s".format(route.parked), style=theme.CRIT)
    return out


def _source_card(group: Group, width: int, detail: bool, pinned: int,
                 snapshot: Snapshot, show_all: bool) -> Panel:
    rows_shown = snapshot.visible_routes(group.routes, show_all)

    title = Text()
    title.append(group.name, style=theme.TITLE)
    if group.priority is not None:
        title.append(" p{}".format(group.priority), style=theme.DIM)

    subtitle = Text()
    subtitle.append("{}/{} 在用".format(group.active, len(group.routes)),
                    style=theme.LABEL if group.active else theme.DIM)
    subtitle.append(" · {}".format(si(group.capacity_tokens, " TPM")),
                    style=theme.DIM)
    if pinned:
        # Sessions carrying encrypted reasoning cannot be moved off the endpoint
        # that produced it. A source with pins is one the balancer has less say
        # over than its weights suggest, which is worth seeing next to them.
        subtitle.append(" · {} pinned".format(pinned), style=theme.ACCENT)
    if group.troubled:
        subtitle.append(" · {} demoted".format(group.troubled),
                        style=theme.WARN)
    hidden = len(group.routes) - len(rows_shown)
    if hidden:
        # Said here as well as in the footer, because `x/y 在用` above counts
        # every deployment on the endpoint: without this the header's total
        # would disagree with the rows underneath it and look like a bug.
        subtitle.append(" · 隐藏 {} 旧".format(hidden), style=theme.DIM)

    body = _route_rows(rows_shown, width - 4, _labels(rows_shown, "source"),
                       {}, detail)
    return Panel(RichGroup(subtitle, body), title=title, width=width,
                 border_style=theme.severity(_load_or_none(group.peak_load)),
                 padding=(0, 1))


def _model_card(group: Group, width: int, detail: bool) -> Panel:
    title = Text()
    title.append(group.name, style=theme.TITLE)

    subtitle = Text()
    subtitle.append("{} 个源".format(len(group.routes)), style=theme.LABEL)
    if group.faces:
        subtitle.append(" · {}".format("+".join(group.faces)), style=theme.DIM)
    subtitle.append(" · {}".format(si(group.capacity_tokens, " TPM")),
                    style=theme.DIM)
    if group.released:
        subtitle.append(" · {}".format(group.released), style=theme.DIM)
    if group.troubled:
        subtitle.append(" · {} demoted".format(group.troubled),
                        style=theme.WARN)

    # The balancer's share of this model's traffic, next to the source name.
    # It is the answer to "how much does this source actually offer" that the
    # capacity figure alone does not give: a big deployment that keeps getting
    # demoted has a large ceiling and a small share.
    shares = {r.key: ("{:.0%}".format(r.share) if r.share is not None else "")
              for r in group.routes}
    body = _route_rows(group.routes, width - 4, _labels(group.routes, "model"),
                       shares, detail)
    return Panel(RichGroup(subtitle, body), title=title, width=width,
                 border_style=theme.severity(_load_or_none(group.peak_load)),
                 padding=(0, 1))


def _load_or_none(value: float) -> Optional[float]:
    return None if value < 0 else value


# --------------------------------------------------------------------------
# boards
# --------------------------------------------------------------------------

def _grid(cards, widths, gap: int = 2) -> Table:
    """Lay one row of cards out so the row fills the width exactly."""
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


def render_groups(groups: List[Group], width: int, height: int, offset: int,
                  sort: str, snapshot: Snapshot, kind: str,
                  show_all: bool = False):
    """A scrollable grid of cards. Returns (renderable, total card rows)."""
    ordered = snapshot.sorted_groups(
        snapshot.visible_groups(groups, show_all), sort)
    if not ordered:
        return Text("no routes — has the probe been run?", style=theme.DIM), 0

    widths = column_widths(width, len(ordered), MIN_CARD)
    per_row = len(widths)
    # A detail line doubles a card's height. Worth it when there is room, and
    # the first thing to go when there is not — the bars stay legible either way.
    card_rows = -(-len(ordered) // per_row)
    detail = height >= card_rows * 3 + 4 or per_row <= 2

    pins = (snapshot.affinity or {}).get("sessions_per_endpoint") or {}
    cards = []
    for index, group in enumerate(ordered):
        card_width = widths[index % per_row]
        if kind == "source":
            cards.append(_source_card(group, card_width, detail,
                                      pins.get(group.name, 0),
                                      snapshot, show_all))
        else:
            cards.append(_model_card(group, card_width, detail))

    banded = rows(cards, per_row)
    visible = banded[offset:]
    return (RichGroup(*[_grid(band, widths[:len(band)]) for band in visible]),
            len(banded))


# --------------------------------------------------------------------------
# events
# --------------------------------------------------------------------------

EVENT_FILTERS = ("全部", "问题", "他人容量")
# Kept in step with proxy/events.py::PROBLEM_KINDS by hand, because the
# dashboard talks to the proxy over HTTP and imports nothing from it. It had
# already drifted once: `unpinned` and `upstream_error` were added on the
# server and the filter here went on hiding exactly the two kinds that say a
# turn was refused for state it was carrying.
PROBLEM_KINDS = frozenset(
    {"throttle", "demote", "failover", "timeout", "exhausted", "stripped",
     "unpinned", "upstream_error"})


def filter_events(events: List[dict], mode: int) -> List[dict]:
    if mode == 1:
        return [e for e in events if e.get("kind") in PROBLEM_KINDS]
    if mode == 2:
        # The only events that carry a revised estimate of what other tenants
        # hold. `foreign` events always do; a throttle does when it moved one.
        return [e for e in events
                if e.get("kind") == "foreign" or e.get("foreign_updated")]
    return events


def render_events(events: List[dict], width: int, height: int, offset: int,
                  filter_mode: int, dropped: bool):
    """Newest first. Returns (renderable, total lines available).

    Newest first rather than a tailing log, because this is read to answer
    "what just went wrong", and a stream that scrolls under the cursor cannot
    be read at all. Nothing auto-follows; the top is always now.

    The rightmost column is fixed and is what the event CHANGED — the revised
    estimate of other tenants, the parking window, the endpoint failed over to.
    It has its own column rather than living at the end of the message because
    when it was part of the message it was the first thing truncated away, and
    it is the most important thing on the line: the message says what happened,
    this says what the proxy now believes differently.
    """
    shown = list(reversed(filter_events(events, filter_mode)))

    change_width = 26 if width >= 100 else 0
    where_width = min(34, max(12, width // 4))

    table = Table.grid(padding=(0, 1), expand=True)
    table.add_column(width=8, no_wrap=True)              # time
    table.add_column(width=1, no_wrap=True)              # mark
    table.add_column(width=9, no_wrap=True)              # kind
    table.add_column(width=where_width, no_wrap=True)    # where
    table.add_column(ratio=1, no_wrap=True)              # what happened
    if change_width:
        table.add_column(width=change_width, no_wrap=True)   # what changed

    def row(*cells):
        table.add_row(*(cells if change_width else cells[:5]))

    if dropped:
        row(Text(""), Text("!", style=theme.WARN),
            Text("gap", style=theme.WARN), Text(""),
            Text("events were dropped: the ring turned over faster than this "
                 "reader read it", style=theme.WARN), Text(""))

    for event in shown[offset:offset + max(1, height)]:
        kind = event.get("kind", "?")
        mark, colour = theme.EVENT_STYLE.get(kind, ("·", theme.DIM))
        stamp = time.strftime("%H:%M:%S", time.localtime(event.get("at", 0)))
        where = event.get("route") or event.get("endpoint") or ""
        model = event.get("model")
        if model and model not in where:
            where = "{} {}".format(where, model).strip()
        row(Text(stamp, style=theme.DIM),
            Text(mark, style=colour),
            Text(kind, style=colour),
            Text(truncate(where, where_width), style=theme.TEXT),
            _event_message(event),
            _event_change(event, change_width))
    return table, len(shown)


def _event_message(event: dict) -> Text:
    """What happened, in the proxy's own words."""
    level = event.get("level", "info")
    style = (theme.CRIT if level == "error"
             else theme.WARN if level == "warning" else theme.LABEL)
    return Text(event.get("message", ""), style=style,
                overflow="ellipsis", no_wrap=True)


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
        before, after = event.get("foreign_before"), event.get("foreign_after")
        if before is not None and after is not None:
            out.append("others ", style=theme.DIM)
            arrow = "→" if after > before else "="
            out.append("{:.0f}%{}{:.0f}%".format(before * 100, arrow,
                                                 after * 100),
                       style=theme.FOREIGN)
            ours = event.get("our_load")
            if ours is not None:
                out.append(" (we {:.0f}%)".format(ours * 100), style=theme.DIM)
            return out

    park = event.get("park_seconds")
    if park:
        out.append("park {:.0f}s".format(park), style=theme.WARN)
        penalty = event.get("penalty")
        if penalty is not None:
            out.append(" ×{:.2f}".format(penalty), style=theme.DIM)
        return out

    to_route = event.get("to_route")
    if to_route:
        out.append("→ ", style=theme.DIM)
        out.append(truncate(to_route, width - 2), style=theme.ACCENT)
        return out

    # Deliberately nothing for an ordinary response. Its duration is already at
    # the end of the message, and repeating it here made every successful line
    # say the same number twice while the column exists to say what CHANGED.
    return out
