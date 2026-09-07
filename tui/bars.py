"""The capacity bar: learned safe RPM and who is spending it.

    ███▓▓▒▒▥····················░░░░
    └ ours ┘└────── free ──────┘└frn┘

The ceiling is the largest RPM this proxy has completed without a rate limit.
It only grows and survives restarts. If a later limit arrives below that value,
the difference is concurrent traffic from others. The bar shows current RPM,
that observed outside RPM, and the remaining part of the learned maximum.

The `ours` part is subdivided by face — chat and Responses, streamed or not,
and the images face — in the same colour, because they are slices of one number
rather than five numbers. See tui/theme.py.

An unknown ceiling draws as neither full nor empty. `load` returns None when
the deployment has never answered, and None means *unknown*: a route that has
not been tried yet cannot report a limit, and the balancer deliberately treats
that as "not busy" so it gets a chance to. A bar that showed it as 0% would be
asserting the deployment is idle, which is a different and unsupported claim.
"""

from typing import Dict, Optional

from rich.text import Text

from . import theme
from .layout import allocate

FACES = ("chat", "chat_stream", "responses", "responses_stream", "image")


def capacity_bar(width: int, by_face: Optional[Dict[str, float]],
                 foreign: float = 0.0) -> Text:
    """A stacked bar `width` cells wide. Segments sum to exactly `width`.

    `by_face` is /routes' `our_load_by_face`, or None when no safe RPM has been
    learned. `foreign` is `foreign_load`. The total this proxy is using is not a
    separate argument: it is the sum of the faces, which /routes builds to
    equal `our_load` exactly. Passing both would be passing the same number
    twice and inviting them to disagree.

    Anything past 100% is scaled down — a deployment can be over its ceiling
    for a moment, and a bar that overflowed its own width would break the
    column alignment for every row beside it.
    """
    bar = Text()
    if width <= 0:
        return bar
    if by_face is None:
        bar.append(theme.UNKNOWN_GLYPH * width, style=theme.DIM)
        return bar

    ours = [max(0.0, by_face.get(name, 0.0)) for name in FACES]
    total = sum(ours) + max(0.0, foreign)
    if total > 1.0:
        # Scale everything down together rather than truncating the tail. The
        # shape of the split is the information here; which segment happened to
        # be drawn last is not.
        ours = [f / total for f in ours]
        foreign = max(0.0, foreign) / total
        used = 1.0
    else:
        used = total

    cells = allocate(width, ours + [max(0.0, foreign), max(0.0, 1.0 - used)])
    for name, n in zip(FACES, cells):
        if n:
            bar.append(theme.FACE_GLYPH.get(name, "█") * n, style=theme.OURS)
    if cells[len(FACES) + 1]:
        bar.append(theme.FREE_GLYPH * cells[len(FACES) + 1], style=theme.FREE)
    # Outside usage stays anchored to the right; reclaiming it opens space
    # toward our traffic on the left.
    if cells[len(FACES)]:
        bar.append(theme.FOREIGN_GLYPH * cells[len(FACES)],
                   style=theme.FOREIGN)
    return bar


def rpm_capacity(value: Optional[float], label: bool = True) -> Text:
    """The learned maximum beside a capacity bar, in requests per minute."""
    out = Text("MAX RPM: " if label else "", style=theme.DIM, no_wrap=True)
    if value is None:
        out.append("—", style=theme.DIM)
    else:
        label = "{:.1f}".format(value) if value < 1000 else si(value)
        out.append(label, style=theme.ACCENT)
    return out


def legend() -> Text:
    """The key. Shown once in the footer, not once per bar."""
    out = Text()
    for name in FACES:
        out.append(theme.FACE_GLYPH[name], style=theme.OURS)
        out.append("{} ".format(theme.FACE_LABEL[name]), style=theme.DIM)
    out.append(theme.FOREIGN_GLYPH, style=theme.FOREIGN)
    out.append("others ", style=theme.DIM)
    out.append(theme.FREE_GLYPH, style=theme.FREE)
    out.append("available ", style=theme.DIM)
    out.append(theme.UNKNOWN_GLYPH, style=theme.DIM)
    out.append("no completed RPM sample", style=theme.DIM)
    return out


def si(value: Optional[float], unit: str = "") -> str:
    """Compact magnitude: 300000 -> 300k. Column width is scarce here."""
    if value is None:
        return "—"
    value = float(value)
    for limit, suffix in ((1e9, "G"), (1e6, "M"), (1e3, "k")):
        if abs(value) >= limit:
            scaled = value / limit
            text = ("{:.0f}" if scaled >= 10 else "{:.1f}").format(scaled)
            return "{}{}{}".format(text, suffix, unit)
    return "{:.0f}{}".format(value, unit)
