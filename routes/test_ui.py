from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from forecast_db import engine
from jobs.generate_forecasts import _build_rows_for_topic
from weather_api import get_forecast_by_coords
from weather_db import extract_weather_from_db
from database import get_tag_specification

router = APIRouter()


class CacheSelectRequest(BaseModel):
    topic: str
    date: str
    limit: int = Field(default=200, ge=1, le=5000)


class GenerateRequest(BaseModel):
    prediction_date: str
    topics: list[str] = Field(default_factory=list)
    mode: str = Field(default="future", pattern="^(future|history)$")
    write: bool = True



@router.get("/test-ui", include_in_schema=False)
def test_ui() -> FileResponse:
    html_path = Path(__file__).resolve().parent.parent / "static" / "test-ui.html"
    return FileResponse(html_path)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
def ready() -> dict[str, str]:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ready"}


@router.get("/admin/db/tables")
def admin_db_tables():
    query = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query).scalars().all()
    return {"tables": rows}


@router.get("/admin/db/stats")
def admin_db_stats():
    query = text(
        """
        SELECT count(*)::bigint AS points_count,
               min(ts) AS min_ts,
               max(ts) AS max_ts,
               count(distinct topic)::bigint AS topics_count
        FROM pv_forecast_points
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query).mappings().first()
    return dict(row or {})


@router.get("/admin/cache/topic-samples")
def admin_cache_topic_samples(
    limit: int = Query(default=50, ge=1, le=5000),
):
    query = text(
        """
        SELECT topic, count(*)::bigint AS points_count, min(ts) AS min_ts, max(ts) AS max_ts
        FROM pv_forecast_points
        GROUP BY topic
        ORDER BY topic
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(query, {"limit": limit}).mappings().all()]
    return {"items": rows}


@router.get("/admin/cache/coverage")
def admin_cache_coverage(topic: str, date: str):
    start = datetime.strptime(date, "%Y-%m-%d")
    end = start + timedelta(days=1)
    query = text(
        """
        SELECT count(*)::bigint AS count, min(ts) AS min_ts, max(ts) AS max_ts
        FROM pv_forecast_points
        WHERE topic = :topic AND ts >= :start_ts AND ts < :end_ts
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"topic": topic, "start_ts": start, "end_ts": end}).mappings().first()
    return {"topic": topic, "date": date, **dict(row or {})}


@router.post("/admin/cache/select")
def admin_cache_select(request: CacheSelectRequest):
    start = datetime.strptime(request.date, "%Y-%m-%d")
    end = start + timedelta(days=1)
    query = text(
        """
        SELECT ts, power
        FROM pv_forecast_points
        WHERE topic = :topic AND ts >= :start_ts AND ts < :end_ts
        ORDER BY ts
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        points = [
            {"x": r["ts"].strftime("%Y-%m-%d %H:%M"), "y": float(r["power"])}
            for r in conn.execute(
                query,
                {
                    "topic": request.topic,
                    "start_ts": start,
                    "end_ts": end,
                    "limit": request.limit,
                },
            ).mappings()
        ]
    return {
        "topic": request.topic,
        "date": request.date,
        "count": len(points),
        "min_ts": points[0]["x"] if points else None,
        "max_ts": points[-1]["x"] if points else None,
        "points": points,
    }


@router.post("/admin/jobs/generate")
def admin_jobs_generate(request: GenerateRequest):
    generated = []
    errors = []
    prediction_date = datetime.strptime(request.prediction_date, "%Y-%m-%d").date()

    all_rows: list[tuple[str, datetime, float]] = []
    for topic in request.topics:
        try:
            spec = get_tag_specification(topic)
            if not spec:
                errors.append({"topic": topic, "error": "Specification not found"})
                continue

            uid = spec.get("sm_user_object_id")
            lat = float(spec.get("latitude", 0.0))
            lon = float(spec.get("longitude", 0.0))

            if request.mode == "future":
                records = get_forecast_by_coords(lat, lon, prediction_date)
            else:
                if uid is None:
                    errors.append({"topic": topic, "error": "sm_user_object_id is missing"})
                    continue
                records = extract_weather_from_db(uid, request.prediction_date)

            rows = _build_rows_for_topic(topic, records)
            all_rows.extend(rows)
            generated.append({"topic": topic, "points": len(rows)})
        except Exception as exc:
            errors.append({"topic": topic, "error": str(exc)})

    written = 0
    if request.write and all_rows:
        insert_sql = text(
            """
            INSERT INTO pv_forecast_points (topic, ts, power)
            VALUES (:topic, :ts, :power)
            ON CONFLICT (topic, ts)
            DO UPDATE SET power = EXCLUDED.power, created_at = now()
            """
        )
        with engine.begin() as conn:
            for topic, ts, power in all_rows:
                conn.execute(insert_sql, {"topic": topic, "ts": ts, "power": power})
        written = len(all_rows)

    return {
        "prediction_date": request.prediction_date,
        "mode": request.mode,
        "write": request.write,
        "generated": generated,
        "written": written,
        "errors": errors,
    }
