from __future__ import annotations

import csv
import os
import time
from typing import List

from app.models import PersonLeadRow


def export_people_csv(*, output_dir: str, job_id: str, people: List[PersonLeadRow]) -> str:
    """
    Writes `data/jobs/<job_id>.<timestamp>.csv` and returns the absolute path.
    """
    jobs_dir = os.path.join(output_dir, "jobs")
    os.makedirs(jobs_dir, exist_ok=True)

    ts = int(time.time())
    csv_path = os.path.join(jobs_dir, f"{job_id}.{ts}.csv")

    fieldnames = [
        "login",
        "profile_url",
        "latest_repo_updated_at",
        "score",
        "pillar_builder",
        "pillar_product",
        "pillar_reach",
        "matched_repos",
        "total_contributions_in_sample",
        "top_signal_repo_stars",
        "field_activity_weighted",
    ]

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for p in people:
            writer.writerow(p.to_csv_dict())

    return csv_path

