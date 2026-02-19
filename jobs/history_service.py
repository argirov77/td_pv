from __future__ import annotations

from datetime import datetime
from threading import Lock
from uuid import uuid4

from jobs.generate_forecasts import run_history


class HistoryJobService:
    def __init__(self) -> None:
        self._lock = Lock()
        self._jobs: dict[str, dict] = {}
        self._running_job_id: str | None = None

    def create_job(self, days: int) -> dict:
        with self._lock:
            if self._running_job_id:
                running_job = self._jobs[self._running_job_id]
                return {
                    "started": False,
                    "job": running_job,
                }

            job_id = str(uuid4())
            job = {
                "id": job_id,
                "state": "queued",
                "days": days,
                "created_at": datetime.utcnow(),
                "started_at": None,
                "finished_at": None,
                "error": None,
            }
            self._jobs[job_id] = job
            self._running_job_id = job_id
            return {
                "started": True,
                "job": job,
            }

    def run_job(self, job_id: str) -> None:
        self._set_state(job_id, state="running", started_at=datetime.utcnow())
        try:
            days = int(self._jobs[job_id]["days"])
            run_history(days=days)
        except Exception as exc:
            self._set_state(job_id, state="failed", error=str(exc), finished_at=datetime.utcnow())
            raise
        else:
            self._set_state(job_id, state="completed", finished_at=datetime.utcnow())
        finally:
            with self._lock:
                if self._running_job_id == job_id:
                    self._running_job_id = None

    def get_job(self, job_id: str) -> dict | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            return dict(job)

    def _set_state(self, job_id: str, **updates: object) -> None:
        with self._lock:
            self._jobs[job_id].update(updates)


history_job_service = HistoryJobService()
