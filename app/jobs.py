from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from app.export_csv import export_people_csv
from app.github_client import GitHubRateLimitError, GithubClient
from app.activity import weighted_recent_public_activity
from app.models import PersonLeadRow, SearchRequest
from app.pillars import (
    builder_signal,
    combine_product_score,
    combine_reach_score,
    followers_reach_01,
    kol_overlap_01,
    product_deep_score,
    product_metadata_score,
)
from app.scoring import obscurity_multiplier, recency_score


@dataclass
class _PersonAgg:
    login: str
    html_url: str
    builder_raw: float
    product_raw: float
    latest_repo_updated_at: Optional[str]
    matched_repos: List[str]
    total_contributions_in_sample: int
    top_signal_repo_stars: int


class JobManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}

    def create_job(self) -> str:
        job_id = str(uuid.uuid4())
        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "status": "queued",
                "progress": 0.0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "message": None,
                "output_csv_path": None,
            }
        return job_id

    def update(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        progress: Optional[float] = None,
        message: Optional[str] = None,
        output_csv_path: Optional[str] = None,
    ) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if status is not None:
                job["status"] = status
            if progress is not None:
                job["progress"] = max(0.0, min(1.0, progress))
            if message is not None:
                job["message"] = message
            if output_csv_path is not None:
                job["output_csv_path"] = output_csv_path

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            # Return a shallow copy to avoid partial reads.
            return dict(job)


def build_repo_query(req: SearchRequest) -> str:
    terms: List[str] = []

    cleaned_topics = [t.strip() for t in (req.topics or []) if t and t.strip()]
    topic_terms: List[str] = []
    for t in cleaned_topics:
        # Allow already-prefixed terms like "topic:ai"
        if t.startswith("topic:"):
            topic_terms.append(t)
        else:
            topic_terms.append(f"topic:{t}")

    if topic_terms:
        terms.append("(" + " OR ".join(topic_terms) + ")")

    if req.free_keywords.strip():
        # Treat as user-provided (they can include quotes/AND/OR themselves).
        terms.append(f"({req.free_keywords.strip()})")

    if req.language:
        terms.append(f"language:{req.language}")

    if req.license:
        terms.append(f"license:{req.license}")

    lo = req.min_stars
    hi = req.max_stars
    if lo is not None and hi is not None:
        terms.append(f"stars:{lo}..{hi}")
    elif lo is not None:
        terms.append(f"stars:>={lo}")
    elif hi is not None:
        terms.append(f"stars:<={hi}")

    if req.repo_created_within_days is not None:
        cutoff = (datetime.now(timezone.utc).date() - timedelta(days=req.repo_created_within_days)).isoformat()
        terms.append(f"created:>={cutoff}")

    if req.repo_pushed_within_days is not None:
        pushed_cutoff = (datetime.now(timezone.utc).date() - timedelta(days=req.repo_pushed_within_days)).isoformat()
        terms.append(f"pushed:>={pushed_cutoff}")

    terms.append("fork:false")

    return " ".join(terms)


def _search_sort_params(repo_search_sort: str) -> Tuple[Optional[str], Optional[str]]:
    if repo_search_sort == "best_match":
        return (None, None)
    if repo_search_sort == "updated_desc":
        return ("updated", "desc")
    if repo_search_sort == "stars_asc":
        return ("stars", "asc")
    if repo_search_sort == "stars_desc":
        return ("stars", "desc")
    return (None, None)


def run_people_search_job(
    *,
    job_id: str,
    req: SearchRequest,
    manager: JobManager,
    data_dir: str,
) -> None:
    """
    Worker function that runs the GitHub sourcing pipeline and writes CSV/JSON to disk.
    """
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN") or ""
    if not token.strip() or token.strip() == "your_personal_access_token_here":
        manager.update(job_id, status="error", message="Missing `GITHUB_TOKEN` in .env")
        return

    now_ts = time.time()
    recency_days = req.recency_days
    dedup_by_login = req.dedup_people_by_login

    query = build_repo_query(req)

    try:
        manager.update(job_id, status="running", progress=0.01, message="Searching repositories...")

        w_sum = float(req.weight_builder) + float(req.weight_product) + float(req.weight_reach)
        if w_sum <= 0:
            manager.update(job_id, status="error", message="Pillar weights must sum to a positive number")
            return
        wb = float(req.weight_builder) / w_sum
        wp = float(req.weight_product) / w_sum
        wr = float(req.weight_reach) / w_sum

        client = GithubClient(token=token)

        # GitHub search "per_page" max is 100.
        per_page = 100
        # We'll cap pages based on desired repo_count.
        max_pages = max(1, int((req.repo_count + per_page - 1) / per_page))

        sort_key, order_key = _search_sort_params(req.repo_search_sort)
        repo_items = client.search_repositories(
            query=query,
            per_page=per_page,
            max_pages=max_pages,
            sort=sort_key,
            order=order_key,
        )

        if not repo_items:
            manager.update(job_id, status="done", progress=1.0, message="No repositories matched the query.")
            return

        # Truncate to requested limit.
        repo_items = repo_items[: req.repo_count]

        manager.update(job_id, status="running", progress=0.05, message=f"Processing {len(repo_items)} repos...")

        people: Dict[str, _PersonAgg] = {}

        unique_people_cap = req.max_unique_people
        deep_product_cache: Dict[str, float] = {}

        for idx, repo in enumerate(repo_items):
            # "updated_at" exists for search repositories.
            updated_at_iso = repo.get("updated_at")
            owner = repo.get("owner") or {}
            owner_login = owner.get("login") or ""
            owner_html_url = owner.get("html_url") or ""

            if req.exclude_org_owned and (owner.get("type") == "Organization"):
                continue

            stargazers_count = int(repo.get("stargazers_count") or 0)
            pop_mult = obscurity_multiplier(stargazers=stargazers_count, enabled=req.early_stage_scoring)

            rec_component = recency_score(updated_at_iso, now_ts=now_ts, recency_days=recency_days)

            repo_full_name = repo.get("full_name") or ""
            repo_html_url = repo.get("html_url") or ""
            repo_short = repo.get("name") or (repo_full_name.split("/")[-1] if repo_full_name else "")

            meta_prod = product_metadata_score(repo)
            if req.deep_product_signals and repo_full_name and owner_login:
                if repo_full_name not in deep_product_cache:
                    readme_txt = client.get_repository_readme_text(owner=owner_login, repo=repo_short)
                    wf_n = client.count_workflow_files(owner=owner_login, repo=repo_short)
                    deep_product_cache[repo_full_name] = product_deep_score(
                        readme_text=readme_txt,
                        workflow_file_count=wf_n,
                    )
                deep_01 = deep_product_cache[repo_full_name]
            else:
                deep_01 = 0.0
            product_repo_01 = combine_product_score(
                metadata_01=meta_prod,
                deep_01=deep_01,
                deep_enabled=req.deep_product_signals,
            )

            contributors: List[Dict[str, Any]] = []
            if owner_login:
                try:
                    contributors = client.get_contributors(
                        owner=owner_login,
                        repo=(repo.get("name") or repo_full_name.split("/")[-1]),
                        per_page=min(100, req.contributors_per_repo),
                        max_pages=1,
                    )
                except Exception:
                    # Some repos may block/limit contributor listing. Keep the owner lead.
                    contributors = []

            # Always include the owner as a "person lead".
            if owner_login:
                owner_builder = builder_signal(
                    recency_01=rec_component,
                    contributions=0,
                    popularity_multiplier=pop_mult,
                    contribution_weight=req.builder_contribution_weight,
                )
                _upsert_person(
                    people=people,
                    login=owner_login,
                    html_url=owner_html_url or f"https://github.com/{owner_login}",
                    latest_repo_updated_at=updated_at_iso,
                    builder_raw=owner_builder,
                    product_raw=product_repo_01,
                    matched_repo=repo_full_name,
                    repo_stars=stargazers_count,
                    contributions_to_add=0,
                    dedup_by_login=dedup_by_login,
                    unique_people_cap=unique_people_cap,
                )

            # Dedup: keep strongest per-login pillar signals.
            for c in contributors[: req.contributors_per_repo]:
                login = c.get("login") or ""
                if not login:
                    continue

                c_builder = builder_signal(
                    recency_01=rec_component,
                    contributions=int(c.get("contributions") or 0),
                    popularity_multiplier=pop_mult,
                    contribution_weight=req.builder_contribution_weight,
                )
                _upsert_person(
                    people=people,
                    login=login,
                    html_url=c.get("html_url") or f"https://github.com/{login}",
                    latest_repo_updated_at=updated_at_iso,
                    builder_raw=c_builder,
                    product_raw=product_repo_01,
                    matched_repo=repo_full_name,
                    repo_stars=stargazers_count,
                    contributions_to_add=int(c.get("contributions") or 0),
                    dedup_by_login=dedup_by_login,
                    unique_people_cap=unique_people_cap,
                )

                if dedup_by_login and len(people) >= unique_people_cap:
                    break

            progress = (idx + 1) / float(max(1, len(repo_items)))
            manager.update(job_id, progress=progress * 0.95, message=f"Progress: {idx+1}/{len(repo_items)} repos")

            if dedup_by_login and len(people) >= unique_people_cap:
                break

        # Rank candidates, optional activity boost (builder), normalize pillars, reach lookups, weighted score.
        sorted_aggs = sorted(
            people.values(),
            key=lambda p: (p.builder_raw + p.product_raw),
            reverse=True,
        )
        limit_people = min(len(sorted_aggs), unique_people_cap)
        sorted_aggs = sorted_aggs[:limit_people]

        activity_by_login: Dict[str, float] = {}
        if req.enrich_field_activity and sorted_aggs:
            window_sec = float(req.activity_window_days) * 86400.0
            cap_a = min(len(sorted_aggs), req.max_people_activity_enrichment)
            manager.update(
                job_id,
                progress=0.92,
                message=f"Builder activity (public events): 0/{cap_a}...",
            )
            for i in range(cap_a):
                agg = sorted_aggs[i]
                if agg.login.endswith("[bot]"):
                    continue
                try:
                    ev = client.get_user_public_events(login=agg.login, per_page=100, max_pages=2)
                except Exception:
                    ev = []
                w = weighted_recent_public_activity(ev, now_ts=now_ts, window_seconds=window_sec)
                activity_by_login[agg.login] = w
                agg.builder_raw += req.activity_boost_per_unit * w * 0.012
                agg.builder_raw = min(agg.builder_raw, 25.0)
                manager.update(
                    job_id,
                    progress=0.92 + 0.03 * ((i + 1) / max(1, cap_a)),
                    message=f"Builder activity (public events): {i+1}/{cap_a}...",
                )

        max_b = max((p.builder_raw for p in sorted_aggs), default=0.0) or 1.0
        max_p = max((p.product_raw for p in sorted_aggs), default=0.0) or 1.0

        kol_set = {k.lower() for k in (req.kol_github_logins or []) if k.strip()}
        reach_raw: Dict[str, float] = {}
        cap_i = min(len(sorted_aggs), req.max_people_reach_lookup)
        manager.update(job_id, progress=0.96, message=f"Reach signals: 0/{cap_i}...")
        for i, agg in enumerate(sorted_aggs):
            if i >= cap_i:
                reach_raw[agg.login] = 0.0
                continue
            prof = client.get_user(login=agg.login)
            if not prof:
                reach_raw[agg.login] = 0.0
                continue
            followers = int(prof.get("followers") or 0)
            f01 = followers_reach_01(followers)
            if kol_set:
                following = client.get_user_following_logins(login=agg.login, per_page=100, max_pages=1)
                k01 = kol_overlap_01(following_logins=set(following), kol_set=kol_set)
                reach_raw[agg.login] = combine_reach_score(
                    followers_01=f01,
                    kol_01=k01,
                    kol_list_nonempty=True,
                    kol_weight_in_reach=req.kol_share_of_reach,
                )
            else:
                reach_raw[agg.login] = combine_reach_score(
                    followers_01=f01,
                    kol_01=0.0,
                    kol_list_nonempty=False,
                )
            manager.update(
                job_id,
                progress=0.96 + 0.02 * ((i + 1) / max(1, cap_i)),
                message=f"Reach signals: {min(i + 1, cap_i)}/{cap_i}...",
            )

        max_r = max(reach_raw.values(), default=0.0) or 1.0

        ranked: List[Tuple[float, float, float, float, _PersonAgg]] = []
        for agg in sorted_aggs:
            b_n = agg.builder_raw / max_b
            p_n = agg.product_raw / max_p
            r_n = reach_raw.get(agg.login, 0.0) / max_r
            sc = wb * b_n + wp * p_n + wr * r_n
            ranked.append((sc, b_n, p_n, r_n, agg))
        ranked.sort(key=lambda x: x[0], reverse=True)

        rows: List[PersonLeadRow] = []
        for sc, b_n, p_n, r_n, p in ranked:
            rows.append(
                PersonLeadRow(
                    login=p.login,
                    html_url=p.html_url,
                    latest_repo_updated_at=p.latest_repo_updated_at,
                    score=sc,
                    matched_repos=p.matched_repos[:5],
                    total_contributions_in_sample=p.total_contributions_in_sample,
                    top_signal_repo_stars=p.top_signal_repo_stars,
                    field_activity_weighted=activity_by_login.get(p.login, 0.0),
                    pillar_builder=b_n,
                    pillar_product=p_n,
                    pillar_reach=r_n,
                )
            )

        manager.update(job_id, progress=0.98, message="Exporting CSV...")

        csv_path = export_people_csv(
            output_dir=data_dir,
            job_id=job_id,
            people=rows,
        )

        json_path = os.path.join(data_dir, "jobs", f"{job_id}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump([r.to_csv_dict() for r in rows], f, ensure_ascii=False, indent=2)

        manager.update(job_id, status="done", progress=1.0, message="Done", output_csv_path=csv_path)

    except GitHubRateLimitError as e:
        manager.update(job_id, status="error", message=str(e))
    except Exception as e:
        manager.update(job_id, status="error", message=f"Job failed: {type(e).__name__}: {e}")


def _upsert_person(
    *,
    people: Dict[str, _PersonAgg],
    login: str,
    html_url: str,
    latest_repo_updated_at: Optional[str],
    builder_raw: float,
    product_raw: float,
    matched_repo: str,
    repo_stars: int,
    contributions_to_add: int,
    dedup_by_login: bool,
    unique_people_cap: int,
) -> None:
    if not dedup_by_login:
        # In this MVP we only support dedup mode for stable CSV.
        # Keeping structure for future expansion.
        return

    if login not in people:
        people[login] = _PersonAgg(
            login=login,
            html_url=html_url,
            builder_raw=builder_raw,
            product_raw=product_raw,
            latest_repo_updated_at=latest_repo_updated_at,
            matched_repos=[matched_repo],
            total_contributions_in_sample=contributions_to_add,
            top_signal_repo_stars=repo_stars,
        )
        return

    agg = people[login]
    agg.total_contributions_in_sample += contributions_to_add
    agg.product_raw = max(agg.product_raw, product_raw)
    if builder_raw > agg.builder_raw:
        agg.builder_raw = builder_raw
        agg.top_signal_repo_stars = repo_stars
        if latest_repo_updated_at:
            agg.latest_repo_updated_at = latest_repo_updated_at
    elif latest_repo_updated_at and not agg.latest_repo_updated_at:
        agg.latest_repo_updated_at = latest_repo_updated_at
    if matched_repo and matched_repo not in agg.matched_repos:
        agg.matched_repos.append(matched_repo)

