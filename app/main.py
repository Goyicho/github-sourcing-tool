from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from app.jobs import JobManager, run_people_search_job
from app.models import JobStatus, SearchRequest


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = str(BASE_DIR / "data")
TEMPLATES_DIR = str(BASE_DIR / "app" / "templates")

manager = JobManager()

app = FastAPI(title="GitHub Sourcing Tool (MVP)")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(os.path.join(TEMPLATES_DIR, "index.html"))


@app.post("/api/search/run", response_model=JobStatus)
def run_search(req: SearchRequest) -> Any:
    job_id = manager.create_job()

    # Start worker thread.
    def _worker() -> None:
        run_people_search_job(job_id=job_id, req=req, manager=manager, data_dir=DATA_DIR)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()

    job = manager.get(job_id)
    return job


@app.get("/api/search/status/{job_id}")
def job_status(job_id: str) -> JSONResponse:
    job = manager.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(job)


@app.get("/api/search/result_json/{job_id}")
def job_result_json(job_id: str) -> JSONResponse:
    json_path = os.path.join(DATA_DIR, "jobs", f"{job_id}.json")
    if not os.path.exists(json_path):
        raise HTTPException(status_code=404, detail="Result JSON not found yet")
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return JSONResponse(payload)


@app.get("/api/search/result_csv/{job_id}")
def job_result_csv(job_id: str) -> FileResponse:
    job = manager.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    csv_path = job.get("output_csv_path")
    if not csv_path or not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="CSV not found yet")
    return FileResponse(csv_path, filename=os.path.basename(csv_path))

