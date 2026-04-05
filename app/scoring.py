from __future__ import annotations

import math
from typing import Optional


def recency_score(
    updated_at_iso: Optional[str],
    *,
    now_ts: float,
    recency_days: int,
) -> float:
    """
    Exponential decay score in 0..1 where 1 is very recent and values smoothly decrease with age.
    """
    if not updated_at_iso:
        return 0.0
    try:
        # GitHub REST returns ISO8601 timestamps like "2024-01-02T03:04:05Z"
        updated_ts = _parse_github_iso(updated_at_iso)
    except ValueError:
        return 0.0

    days_since = (now_ts - updated_ts) / 86400.0
    if days_since < 0:
        days_since = 0
    if recency_days <= 0:
        return 0.0
    # At `recency_days`, score is exp(-1) ~ 0.37; further decay continues smoothly.
    return max(0.0, min(1.0, math.exp(-(days_since / float(recency_days)))))


def obscurity_multiplier(*, stargazers: int, enabled: bool) -> float:
    """
    Down-weight signals from mega-star repos (common OSS) when early-stage scoring is on.
    Returns a multiplier in (0, 1], ~1 for small repos and smaller for very large star counts.
    """
    if not enabled:
        return 1.0
    if stargazers <= 0:
        return 1.0
    return 1.0 / (1.0 + 0.45 * math.log10(float(stargazers) + 1.0))


def person_score(
    *,
    recency_component: float,
    contributions_total: int,
    popularity_multiplier: float = 1.0,
) -> float:
    """
    Combine recency (dominant) with contributions as a small tie-breaker.
    """
    tie_break = math.log10(contributions_total + 1)
    base = (recency_component * 1000.0) + tie_break
    return base * popularity_multiplier


def _parse_github_iso(value: str) -> float:
    # Fast path for the common Z format
    # e.g. "2024-06-01T12:34:56Z"
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    # Python's datetime.fromisoformat handles "+00:00"
    import datetime as _dt

    dt = _dt.datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.timestamp()

