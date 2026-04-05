from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, List

# GitHub public event types — higher weight = stronger signal someone is actively shipping / engaging.
_EVENT_WEIGHTS: Dict[str, float] = {
    "PushEvent": 2.0,
    "PullRequestEvent": 2.0,
    "PullRequestReviewEvent": 1.6,
    "PullRequestReviewCommentEvent": 1.2,
    "IssuesEvent": 1.3,
    "IssueCommentEvent": 1.0,
    "CreateEvent": 0.9,
    "ReleaseEvent": 1.5,
    "ForkEvent": 0.5,
    "WatchEvent": 0.25,
    "PublicEvent": 0.3,
    "DeleteEvent": 0.2,
}


def _github_iso_to_ts(value: str) -> float:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = _dt.datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.timestamp()


def weighted_recent_public_activity(
    events: List[Dict[str, Any]],
    *,
    now_ts: float,
    window_seconds: float,
) -> float:
    """
    Sum weighted event scores whose created_at falls within [now - window, now].
    """
    cutoff = now_ts - window_seconds
    total = 0.0
    for e in events:
        created = e.get("created_at")
        if not created or not isinstance(created, str):
            continue
        try:
            t = _github_iso_to_ts(created)
        except ValueError:
            continue
        if t < cutoff:
            continue
        typ = (e.get("type") or "") or ""
        w = _EVENT_WEIGHTS.get(typ, 0.12)
        total += w
    return total
