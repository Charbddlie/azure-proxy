"""Retention limits for diagnostic history; durable routing state is retained."""

RETENTION_SECONDS = 24 * 3600
CLEANUP_INTERVAL = 60


def recent(events, cutoff):
    return [event for event in events if event.get("at", 0) > cutoff]


def prune_checkpoint(saved, cutoff):
    """Expire raw attempt history while keeping learned values and the cursor."""
    saved["pending"] = {key: value for key, value in saved.get("pending", {}).items()
                        if value.get("entry") and value["entry"][0] > cutoff}
    for state in saved.get("states", {}).values():
        state["sent"] = [entry for entry in state.get("sent", []) if entry[0] > cutoff]
    if "events" in saved:
        saved["events"] = recent(saved["events"], cutoff)
