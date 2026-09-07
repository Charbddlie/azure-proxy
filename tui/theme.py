"""The palette and the glyphs, in one place because they are one decision.

Low saturation throughout. A dashboard that is looked at for hours has to be
readable rather than loud, and — more to the point here — saturation is the
only channel left to mean *urgency*. If the normal state is already vivid, a
throttled endpoint has nothing louder to be.

Two rules the rest of the package follows:

**One hue per quantity.** Request faces share the same sage green and use
different block textures to distinguish chat, Responses, streaming and images.

**Colour states, not values.** The severity colours are applied to the
percentage and the panel border, never to the bar body. The bar's job is to
show proportion; recolouring it at a threshold makes the same length mean two
different things depending on where the boundary happens to fall.
"""

# -- palette ---------------------------------------------------------------
#
# Truecolor hex. Every terminal this is meant for reports COLORTERM=truecolor;
# rich degrades to the nearest 256-colour cell on the ones that do not, and the
# palette is chosen so that degradation still separates the four roles.

BORDER = "#3a3f46"          # panel edges at rest
BORDER_ACTIVE = "#55606b"   # the panel under the cursor
TITLE = "#9aa7b4"           # panel titles
LABEL = "#7c8894"           # field names, units
DIM = "#4e565f"             # things present but not currently interesting
TEXT = "#c3cad2"            # ordinary values

OURS = "#7fa08a"            # sage: this proxy's current RPM
FOREIGN = "#b39a72"         # tan: outside RPM observed at the last throttle
FREE = "#3b4149"            # the rest of the learned safe RPM

OK = "#86a98e"
WARN = "#c2a06a"            # muted amber
CRIT = "#b57f83"            # muted rose
ACCENT = "#8d9bc4"          # periwinkle: selection, the active board
PINNED = "#bdcfea"          # pale blue: bound families, lighter than RPM values

# -- severity --------------------------------------------------------------
#
# Thresholds on TOTAL load — ours plus theirs — because that is what decides
# whether the next request gets a 429, and 30% of a deployment someone else has
# 60% of is a busier place to send traffic than 50% of an empty one.
#
# 0.7 is the shipping spill_threshold (settings/policy.yaml). Sitting exactly on
# it is the priority_threshold mode working correctly, not a problem, so the
# warning band opens below it and red is kept for genuinely over.

WARN_AT = 0.55
CRIT_AT = 0.80


def severity(total_load):
    """The colour for a load figure. `None` means unknown, which is not zero."""
    if total_load is None:
        return DIM
    if total_load >= CRIT_AT:
        return CRIT
    if total_load >= WARN_AT:
        return WARN
    return OK


# -- glyphs ----------------------------------------------------------------
#
# Distinct single-cell block textures, all drawn in the original OURS green.
#
# Keys match the face names used by snapshots and the capacity bar.

FACE_GLYPH = {
    "chat": "█",
    "chat_stream": "▓",
    "responses": "▒",
    "responses_stream": "▚",
    "image": "▞",
}
FACE_LABEL = {
    "chat": "chat",
    "chat_stream": "chat~",
    "responses": "resp",
    "responses_stream": "resp~",
    "image": "img",
}
FOREIGN_GLYPH = "░"
FREE_GLYPH = "·"
UNKNOWN_GLYPH = "─"          # the ceiling is not known, so nothing can be shown

# -- event stream ----------------------------------------------------------
#
# One mark per kind, coloured by what the kind MEANS rather than by its log
# level. A `foreign` event is logged at info because it is not an error, but it
# is the most interesting line in the stream — it is the only moment the proxy
# can see the other tenants at all — so it gets its own colour rather than
# sinking into the ordinary traffic.

EVENT_STYLE = {
    "boot":      ("•", ACCENT),
    "request":   ("→", DIM),
    "response":  ("←", DIM),
    "capacity":  ("↑", OK),
    "throttle":  ("▲", CRIT),
    "demote":    ("▼", WARN),
    "foreign":   ("◆", FOREIGN),
    "failover":  ("⇄", WARN),
    "held":      ("⏸", ACCENT),
    "pin":       ("⚲", ACCENT),
    "inherited": ("⚯", ACCENT),
    "unpinned":  ("⚠", WARN),
    "upstream_error": ("✖", CRIT),
    "stripped":  ("✂", CRIT),
    "timeout":   ("◷", WARN),
    "exhausted": ("✖", CRIT),
    "image_tool": ("▣", ACCENT),
    "token":     ("⚿", LABEL),
}
