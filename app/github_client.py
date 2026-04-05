from __future__ import annotations

import base64
import datetime as dt
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests


class GitHubRateLimitError(RuntimeError):
    pass


@dataclass
class RateLimitInfo:
    resource: str
    limit: int
    remaining: int
    reset_epoch: int


class GithubClient:
    def __init__(self, *, token: str, timeout_s: float = 20.0) -> None:
        self.token = token
        self.timeout_s = timeout_s
        self.session = requests.Session()
        # Avoid picking up local HTTPS proxy env vars that can break GitHub API calls.
        self.session.trust_env = False
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"token {token}",
                "User-Agent": "vc-github-sourcing-tool",
            }
        )

    def _raise_for_rate_limit(self, resp: requests.Response) -> None:
        resource = (resp.headers.get("X-RateLimit-Resource") or "unknown").lower()
        remaining = resp.headers.get("X-RateLimit-Remaining")
        limit = resp.headers.get("X-RateLimit-Limit")
        reset = resp.headers.get("X-RateLimit-Reset")

        if remaining is None or limit is None or reset is None:
            return

        info = RateLimitInfo(
            resource=resource,
            limit=int(limit),
            remaining=int(remaining),
            reset_epoch=int(reset),
        )

        # Endpoint-aware low-water mark.
        # Search endpoints have low/minute limits, while core endpoints are usually high/hour.
        if info.resource == "search":
            stop_threshold = 2
        elif info.resource == "core":
            stop_threshold = 10
        else:
            stop_threshold = 5

        if info.remaining <= stop_threshold:
            reset_in = max(0, info.reset_epoch - int(time.time()))
            reset_at_utc = dt.datetime.fromtimestamp(info.reset_epoch, tz=dt.timezone.utc).strftime("%H:%M:%S UTC")
            raise GitHubRateLimitError(
                f"GitHub rate limit almost exhausted for `{info.resource}`: "
                f"{info.remaining}/{info.limit} remaining. "
                f"Resets in ~{reset_in}s (at {reset_at_utc})."
            )

    def _get(self, url: str, *, params: Optional[Dict] = None) -> Dict:
        resp = self.session.get(url, params=params, timeout=self.timeout_s)
        self._raise_for_rate_limit(resp)
        resp.raise_for_status()
        return resp.json()

    def search_repositories(
        self,
        *,
        query: str,
        per_page: int,
        max_pages: int,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> List[Dict]:
        all_items: List[Dict] = []
        for page in range(1, max_pages + 1):
            params: Dict[str, object] = {
                "q": query,
                "per_page": per_page,
                "page": page,
            }
            if sort:
                params["sort"] = sort
                params["order"] = order or "desc"
            payload = self._get(
                "https://api.github.com/search/repositories",
                params=params,
            )
            items = payload.get("items") or []
            all_items.extend(items)
            if len(items) < per_page:
                break
        return all_items

    def get_contributors(
        self,
        *,
        owner: str,
        repo: str,
        per_page: int,
        max_pages: int,
    ) -> List[Dict]:
        all_items: List[Dict] = []
        for page in range(1, max_pages + 1):
            payload = self._get(
                f"https://api.github.com/repos/{owner}/{repo}/contributors",
                params={
                    "per_page": per_page,
                    "page": page,
                    # Prefer real users over anonymous contributions.
                    "anon": 0,
                },
            )
            items = payload or []
            all_items.extend(items)
            if len(items) < per_page:
                break
        return all_items

    def get_user(self, *, login: str) -> Optional[Dict]:
        try:
            payload = self._get(f"https://api.github.com/users/{login}", params=None)
            return payload
        except requests.HTTPError:
            return None

    def get_repository_readme_text(self, *, owner: str, repo: str) -> Optional[str]:
        """Decodes default-branch README when present."""
        resp = self.session.get(
            f"https://api.github.com/repos/{owner}/{repo}/readme",
            timeout=self.timeout_s,
        )
        self._raise_for_rate_limit(resp)
        if resp.status_code == 404:
            return None
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None
        content = data.get("content")
        if not content or data.get("encoding") != "base64":
            return None
        try:
            raw = base64.b64decode(content.replace("\n", ""))
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return None

    def count_workflow_files(self, *, owner: str, repo: str) -> int:
        """Counts YAML workflow files under .github/workflows (GitHub Actions)."""
        resp = self.session.get(
            f"https://api.github.com/repos/{owner}/{repo}/contents/.github/workflows",
            timeout=self.timeout_s,
        )
        self._raise_for_rate_limit(resp)
        if resp.status_code == 404:
            return 0
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            return 0
        data = resp.json()
        if not isinstance(data, list):
            return 0
        n = 0
        for item in data:
            if not isinstance(item, dict):
                continue
            name = (item.get("name") or "").lower()
            if name.endswith((".yml", ".yaml")):
                n += 1
        return n

    def get_user_following_logins(self, *, login: str, per_page: int = 100, max_pages: int = 1) -> List[str]:
        out: List[str] = []
        pp = min(max(1, per_page), 100)
        for page in range(1, max_pages + 1):
            resp = self.session.get(
                f"https://api.github.com/users/{login}/following",
                params={"per_page": pp, "page": page},
                timeout=self.timeout_s,
            )
            self._raise_for_rate_limit(resp)
            if resp.status_code == 404:
                break
            try:
                resp.raise_for_status()
            except requests.HTTPError:
                break
            chunk = resp.json()
            if not isinstance(chunk, list):
                break
            for u in chunk:
                if isinstance(u, dict) and u.get("login"):
                    out.append(str(u["login"]).lower())
            if len(chunk) < pp:
                break
        return out

    def get_user_public_events(self, *, login: str, per_page: int, max_pages: int) -> List[Dict]:
        """
        Recent public events for a user (what they did on GitHub — pushes, PRs, issues, etc.).
        Uses core rate limit; capped by max_pages.
        """
        out: List[Dict] = []
        pp = min(max(1, per_page), 100)
        for page in range(1, max_pages + 1):
            resp = self.session.get(
                f"https://api.github.com/users/{login}/events/public",
                params={"per_page": pp, "page": page},
                timeout=self.timeout_s,
            )
            self._raise_for_rate_limit(resp)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
            chunk = resp.json()
            if not isinstance(chunk, list):
                break
            out.extend(chunk)
            if len(chunk) < pp:
                break
        return out

