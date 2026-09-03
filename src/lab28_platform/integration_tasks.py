"""Four student-owned boundaries used by the live platform.

Run ``uv run pytest starter-tests -q`` while completing these functions.  Do
not change their signatures: Kafka, Delta, Feast and ``/ready`` call them.
"""

from __future__ import annotations

from collections.abc import Iterable
from operator import attrgetter
from typing import Any

from lab28_platform.contracts import FEATURE_REFS, IngestionEvent


def event_headers(
    traceparent: str | None, idempotency_key: str
) -> list[tuple[str, bytes]]:
    """Return byte-valued Kafka headers for trace and replay correlation.

    ``idempotency-key`` is always required.  Omit ``traceparent`` when no trace
    is active rather than sending an empty, invalid W3C header.
    """
    headers: list[tuple[str, bytes]] = [("idempotency-key", idempotency_key.encode())]
    if traceparent is not None:
        headers.append(("traceparent", traceparent.encode()))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    # Sort descending by (occurred_at, event_id) to get newest first
    # then deduplicate by idempotency_key (first occurrence wins after sort)
    seen: dict[str, IngestionEvent] = {}
    for event in sorted(events, key=attrgetter("occurred_at", "event_id"), reverse=True):
        if event.idempotency_key not in seen:
            seen[event.idempotency_key] = event
    # Return in deterministic key order
    return [seen[key] for key in sorted(seen)]


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    return {
        "entities": {"asker_id": [asker_id]},
        "features": list(FEATURE_REFS),
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    probes_list = list(probes)
    # Check mandatory failures first
    if any(p.get("mandatory", False) and not p.get("ready", False) for p in probes_list):
        return "not_ready"
    # Check optional failures
    if any(not p.get("ready", False) for p in probes_list):
        return "degraded"
    return "ready"
