from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


RepoSearchSort = Literal["best_match", "updated_desc", "stars_asc", "stars_desc"]


class SearchRequest(BaseModel):
    topics: List[str] = Field(default_factory=list, description="Comma-separated topics/tags (will be converted to `topic:<t>` query terms).")
    free_keywords: str = Field(default="", description="Optional raw keywords to include in the GitHub search query.")
    language: Optional[str] = None
    license: Optional[str] = None
    min_stars: Optional[int] = Field(default=None, ge=0)
    max_stars: Optional[int] = Field(
        default=800,
        description="Cap repo stars (excludes mega-popular OSS). Set null / omit to disable.",
    )
    repo_created_within_days: Optional[int] = Field(
        default=None,
        ge=1,
        description="Only repos created on or after (today - N days). Tightens to younger projects.",
    )
    exclude_org_owned: bool = Field(
        default=False,
        description="Skip repositories owned by a GitHub Organization (often large OSS foundations).",
    )
    early_stage_scoring: bool = Field(
        default=True,
        description="Down-rank people leads that come from very high-star repositories.",
    )
    repo_search_sort: RepoSearchSort = Field(
        default="stars_asc",
        description="Repository search ordering. `stars_asc` surfaces lower-star repos first (good for early-stage).",
    )

    recency_days: int = Field(default=180, ge=1, description="Decay horizon for repo `updated_at` in scoring.")

    repo_pushed_within_days: Optional[int] = Field(
        default=None,
        ge=1,
        description="Search qualifier `pushed:>=date` — keeps repos with recent commits (field momentum).",
    )

    enrich_field_activity: bool = Field(
        default=False,
        description="If true, fetches public event timelines for top leads (extra core API calls) and boosts the builder pillar.",
    )
    activity_window_days: int = Field(default=21, ge=1, le=90, description="How far back to count public events.")
    max_people_activity_enrichment: int = Field(
        default=80,
        ge=1,
        le=400,
        description="Only enrich the top N people by builder+product pre-rank (controls API cost).",
    )
    activity_boost_per_unit: float = Field(
        default=12.0,
        ge=0,
        description="Additive boost to the builder pillar before normalization (× weighted public events).",
    )

    # Three-pillar scoring (weights are normalized to sum to 1 on each request)
    weight_builder: float = Field(default=1.0, ge=0, description="Builder (recency, contributions, activity).")
    weight_product: float = Field(default=1.0, ge=0, description="Product (README, CI, metadata).")
    weight_reach: float = Field(default=1.0, ge=0, description="Reach (followers, KOL overlap).")

    builder_contribution_weight: float = Field(
        default=0.22,
        ge=0,
        le=1,
        description="How much commit volume matters inside the builder pillar (lower = less pure-contributor bias).",
    )

    kol_github_logins: List[str] = Field(
        default_factory=list,
        description="GitHub logins treated as KOLs; overlap with a user's following boosts reach (first page only).",
    )
    deep_product_signals: bool = Field(
        default=False,
        description="Fetch README + .github/workflows per repo (2 extra API calls per repo when enabled).",
    )
    max_people_reach_lookup: int = Field(
        default=200,
        ge=1,
        le=800,
        description="How many top candidates get /users and optional /following calls for the reach pillar.",
    )
    kol_share_of_reach: float = Field(
        default=0.45,
        ge=0,
        le=1,
        description="When KOL list is non-empty: weight of KOL overlap vs followers inside the reach pillar.",
    )

    # Limits
    repo_count: int = Field(default=200, ge=1)
    contributors_per_repo: int = Field(default=20, ge=1)
    dedup_people_by_login: bool = True

    # Safety/UX
    max_unique_people: int = Field(default=800, ge=1, description="Stop after deduplicated people reach this cap.")

    @field_validator("max_stars", mode="before")
    @classmethod
    def max_stars_non_negative(cls, v: Any) -> Any:
        if v is not None and isinstance(v, int) and v < 0:
            raise ValueError("max_stars must be >= 0")
        return v

    @model_validator(mode="after")
    def validate_star_range(self) -> "SearchRequest":
        if self.min_stars is not None and self.max_stars is not None and self.max_stars < self.min_stars:
            raise ValueError("max_stars must be greater than or equal to min_stars when both are set")
        return self

    @model_validator(mode="after")
    def pillar_weights_positive_sum(self) -> "SearchRequest":
        s = float(self.weight_builder) + float(self.weight_product) + float(self.weight_reach)
        if s <= 0:
            raise ValueError("At least one of weight_builder, weight_product, weight_reach must be > 0")
        return self

    @field_validator("kol_github_logins", mode="before")
    @classmethod
    def clean_kol_logins(cls, v: Any) -> Any:
        if v is None:
            return []
        if not isinstance(v, list):
            return []
        out: List[str] = []
        for x in v:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out


class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: float = 0.0
    created_at: datetime
    message: Optional[str] = None
    output_csv_path: Optional[str] = None


@dataclass
class ContributorRow:
    login: str
    html_url: str
    contributions: int


@dataclass
class RepoRow:
    full_name: str
    html_url: str
    owner_login: str
    owner_html_url: str
    updated_at: Optional[str]
    stargazers_count: int
    forks_count: int
    language: Optional[str]


@dataclass
class PersonLeadRow:
    login: str
    html_url: str
    latest_repo_updated_at: Optional[str]
    score: float
    matched_repos: List[str]
    total_contributions_in_sample: int
    top_signal_repo_stars: int = 0
    field_activity_weighted: float = 0.0
    pillar_builder: float = 0.0
    pillar_product: float = 0.0
    pillar_reach: float = 0.0

    def to_csv_dict(self) -> Dict[str, Any]:
        return {
            "login": self.login,
            "profile_url": self.html_url,
            "latest_repo_updated_at": self.latest_repo_updated_at or "",
            "score": round(self.score, 6),
            "pillar_builder": round(self.pillar_builder, 6),
            "pillar_product": round(self.pillar_product, 6),
            "pillar_reach": round(self.pillar_reach, 6),
            "matched_repos": ";".join(self.matched_repos),
            "total_contributions_in_sample": self.total_contributions_in_sample,
            "top_signal_repo_stars": self.top_signal_repo_stars,
            "field_activity_weighted": round(self.field_activity_weighted, 4),
        }

