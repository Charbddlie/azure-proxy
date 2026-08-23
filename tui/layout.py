"""Fitting cards into a terminal of unknown width, exactly.

Two separate problems, and conflating them is what makes a resizable dashboard
look broken:

**How many columns.** As many as fit at the minimum readable card width, capped
at the number of cards — three cards in five columns leaves two holes and looks
like something failed to load.

**What the leftover cells do.** A width of 100 across 3 columns is 33 each with
1 left over. Dropping that cell leaves a one-column gutter down the right edge
that appears and disappears as the terminal is resized, which reads as a
rendering bug. Handing all of it to one column makes that column visibly wider.
So the remainder is spread one cell at a time from the left: at most one cell of
difference between any two columns, and the row fills to the last cell every
time.
"""

from typing import List

from rich.cells import cell_len


def column_widths(width: int, count: int, min_card: int = 46,
                  gap: int = 2) -> List[int]:
    """Widths for a row of `count` cards across `width` cells.

    `sum(result) + gap * (len(result) - 1) == width`, always. Below one card's
    minimum the single column is given the whole width and the card is expected
    to degrade rather than be clipped — a narrow terminal should show less per
    card, not the left 40% of each card.
    """
    if count <= 0 or width <= 0:
        return []
    fits = (width + gap) // (min_card + gap)
    columns = max(1, min(count, fits))
    usable = width - gap * (columns - 1)
    if usable < columns:            # pathologically narrow; one column, as-is
        return [max(1, width)]
    base, extra = divmod(usable, columns)
    return [base + (1 if i < extra else 0) for i in range(columns)]


def rows(items: list, per_row: int) -> List[list]:
    """Chop a list into rows of at most `per_row`."""
    if per_row <= 0:
        return [items] if items else []
    return [items[i:i + per_row] for i in range(0, len(items), per_row)]


def allocate(total: int, fractions: List[float]) -> List[int]:
    """Split `total` cells among `fractions` by largest remainder.

    Returns `sum(result) == total` when the fractions sum to one, which is how
    the bar always calls it: the free remainder is passed in as the last
    fraction precisely so the split is of a whole. Fractions summing to less
    than one allocate proportionally less and leave the bar short, which is the
    honest answer to being asked for less than a full bar.

    Largest remainder rather than rounding each independently, because
    independent rounding does not sum to the total — and a stacked bar whose
    segments do not sum to its width has a gap in it that means nothing. A
    fraction of zero is never given a cell, however many are going spare: an
    empty segment drawn one cell wide is a claim that something is there.

    Then a visibility pass: any fraction that is real but rounds to nothing
    takes one cell from the largest segment. This is not a rounding nicety, it
    is the main thing the bar is asked. Four requests against a 300k-token
    ceiling is 1e-5 of it, so on a 50-cell bar every one of the four face
    segments floors to zero and the empty remainder takes all fifty — a bar
    that says "nothing is happening here" about the route that is, in fact,
    serving the traffic. The borrow costs the free segment a few percent of its
    length and is only ever taken from the segment that can most afford it.
    """
    if total <= 0 or not fractions:
        return [0] * len(fractions)
    positive = [max(0.0, f) for f in fractions]
    # The postcondition has to hold for any input, not only well-formed ones.
    # Fractions adding to more than a whole are scaled rather than truncated:
    # they arrive that way when a deployment is briefly over its ceiling, and a
    # bar that ran past its own width would break the column alignment of every
    # row beside it.
    overflow = sum(positive)
    if overflow > 1.0:
        positive = [f / overflow for f in positive]
    exact = [f * total for f in positive]
    floors = [int(e) for e in exact]

    spare = total - sum(floors)
    if spare > 0:
        eligible = [i for i, e in enumerate(exact) if e > 0]
        eligible.sort(key=lambda i: exact[i] - int(exact[i]), reverse=True)
        for i in eligible[:spare]:
            floors[i] += 1

    for i, value in enumerate(exact):
        if value <= 0 or floors[i] > 0:
            continue
        donor = max(range(len(floors)), key=lambda j: floors[j])
        if floors[donor] < 2:
            break               # nothing left that can spare a cell
        floors[donor] -= 1
        floors[i] += 1
    return floors


def truncate(text: str, width: int) -> str:
    """Cut to `width` terminal cells, marking the cut.

    Cells, not characters. A CJK glyph occupies two columns, so `len()` under-
    counts it by half and a label measured that way overruns the column it was
    supposed to fit in. The route and model names Azure hands out are ASCII
    today, where the two agree — this is here so that the day one of them is
    not, the layout does not quietly come apart.
    """
    if width <= 0:
        return ""
    if cell_len(text) <= width:
        return text
    if width == 1:
        return "…"
    out = ""
    used = 0
    for char in text:
        size = cell_len(char)
        if used + size > width - 1:
            break
        out += char
        used += size
    return out + "…"
