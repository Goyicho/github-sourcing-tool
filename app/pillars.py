from __future__ import annotations

import math
import re
from typing import Any, Dict, Optional, Set


def builder_signal(
    *,
    recency_01: float,
    contributions: int,
    popularity_multiplier: float,
    contribution_weight: float,
) -> float:
    """
    Builder pillar in ~[0, 1]. Recency dominates; contributions are damped (sqrt) so pure commit-count does not dominate.
    """
    gamma = max(0.0, min(1.0, contribution_weight))
    contrib_01 = math.sqrt(min(max(contributions, 0), 2000) / 2000.0)
    blend = (recency_01 * (1.0 - gamma)) + (contrib_01 * gamma)
    out = max(0.0, min(1.0, blend)) * max(0.0, min(1.0, popularity_multiplier))
    return max(0.0, min(1.0, out))


def product_metadata_score(repo: Dict[str, Any]) -> float:
    """
    Product pillar: signals available from search/list repo JSON (no extra API calls).
    """
    desc = (repo.get("description") or "").strip()
    part = min(len(desc) / 350.0, 1.0) * 0.32

    home = 0.18 if (repo.get("homepage") or "").strip() else 0.0

    topics = repo.get("topics") or []
    if not isinstance(topics, list):
        topics = []
    part += min(len(topics) / 7.0, 1.0) * 0.18

    part += 0.14 if repo.get("license") else 0.0

    if repo.get("has_issues", True):
        part += 0.06
    if repo.get("has_discussions"):
        part += 0.05
    if repo.get("has_wiki"):
        part += 0.04

    return max(0.0, min(1.0, part))


_README_SECTION_HINTS = (
    "install",
    "getting started",
    "quickstart",
    "usage",
    "roadmap",
    "pricing",
    "contribut",
    "license",
    "api",
    "architecture",
    "deploy",
)


def product_deep_score(*, readme_text: Optional[str], workflow_file_count: int) -> float:
    """
    Product pillar: extra signal from README body and GitHub Actions workflows (0..1).
    """
    deep = 0.0
    if readme_text:
        L = len(readme_text.strip())
        deep += min(L / 8000.0, 0.5)
        low = readme_text.lower()
        for hint in _README_SECTION_HINTS:
            if hint in low:
                deep += 0.025
        # Light reward for common “product” doc structure
        if re.search(r"^#+\s+", readme_text, re.MULTILINE):
            deep += 0.04
        deep = min(deep, 0.72)

    deep += min(workflow_file_count * 0.1, 0.35)
    return max(0.0, min(1.0, deep))


def combine_product_score(
    *,
    metadata_01: float,
    deep_01: float,
    deep_enabled: bool,
) -> float:
    if not deep_enabled:
        return max(0.0, min(1.0, metadata_01))
    # README / CI adds “product + shipping” signal on top of listing metadata.
    return max(0.0, min(1.0, metadata_01 * 0.42 + deep_01 * 0.58))


def followers_reach_01(followers: int) -> float:
    if followers <= 0:
        return 0.0
    return max(0.0, min(1.0, math.log10(followers + 1.0) / math.log10(100_001.0)))


def kol_overlap_01(*, following_logins: Set[str], kol_set: Set[str]) -> float:
    if not kol_set:
        return 0.0
    overlap = len(following_logins & kol_set)
    # Saturate after a few matches so one lucky follow does not dominate.
    denom = max(5, min(25, len(kol_set) * 2))
    return max(0.0, min(1.0, overlap / float(denom)))


def combine_reach_score(
    *,
    followers_01: float,
    kol_01: float,
    kol_list_nonempty: bool,
    kol_weight_in_reach: float = 0.45,
) -> float:
    kw = max(0.0, min(1.0, kol_weight_in_reach))
    if kol_list_nonempty:
        return max(0.0, min(1.0, (1.0 - kw) * followers_01 + kw * kol_01))
    return followers_01
