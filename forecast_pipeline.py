from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from fastapi import HTTPException
from sqlalchemy import text

from config import load_settings
from database import engine_spec
from forecast_db import bulk_upsert_points, engine as forecast_engine, ensure_month_partitions
from model_loader import load_model
from production import calculate_system_production
from radiation import calculate_panel_irradiance
from weather_api import get_forecast_by_coords
from weather_db import extract_weather_from_db

settings = load_settings()
THRESHOLD_RADIATION = 40
TZ = "Europe/Nicosia"

JOBS: dict[str, dict[str, Any]] = {}


@dataclass
class TopicSpec:
    topic: str
    user_object_id: int | None
    lat: float | None
    lon: float | None
    tilt: float
    azimuth: float
    panel_count_or_kwp: float
    module_length: float | None
    module_width: float | None
    module_efficiency: float
    total_panels: int
    commissioning_date: str | None
    degradation_rate: float
    has_model_file: bool
    errors: list[str]


def _first(spec: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in spec and spec[key] is not None:
            return spec[key]
    return default


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def get_topic_spec(topic: str) -> TopicSpec | None:
    with engine_spec.connect() as conn:
        row = conn.execute(text("SELECT * FROM tag_specification WHERE tag = :topic LIMIT 1"), {"topic": topic}).mappings().first()
    if not row:
        return None

    spec = dict(row)
    model_tag = topic.replace("/", "_")
    from pathlib import Path

    model_dir = Path(__file__).resolve().parent / "Model"
    model_path = model_dir / f"{model_tag}_model.pkl"
    fallback = model_dir / "P0063H01_E001_model.pkl"
    has_model = model_path.exists() or fallback.exists()

    uid = _to_int(_first(spec, ["sm_user_object_id", "user_object_id", "object_id"]))
    lat = _to_float(_first(spec, ["latitude", "lat"]))
    lon = _to_float(_first(spec, ["longitude", "lon", "lng"]))
    tilt = _to_float(_first(spec, ["tilt", "panel_tilt"], 0.0)) or 0.0
    azimuth = _to_float(_first(spec, ["azimuth", "panel_azimuth"], 180.0)) or 180.0
    panel_count_or_kwp = _to_float(_first(spec, ["total_panels", "kwp", "capacity_kwp"], 0.0)) or 0.0

    module_length = _to_float(_first(spec, ["module_length"]))
    module_width = _to_float(_first(spec, ["module_width"]))
    module_eff = _to_float(_first(spec, ["module_efficiency"], 17.7)) or 17.7
    total_panels = _to_int(_first(spec, ["total_panels"], 0)) or 0
    comm = _first(spec, ["commissioning_date"])
    comm_str = str(comm)[:10] if comm else None
    degr = _to_float(_first(spec, ["degradation_rate"], 0.0)) or 0.0

    errors = []
    if uid is None:
        errors.append("missing user_object_id")
    if lat is None or lon is None:
        errors.append("missing coordinates")

    return TopicSpec(
        topic=topic,
        user_object_id=uid,
        lat=lat,
        lon=lon,
        tilt=tilt,
        azimuth=azimuth,
        panel_count_or_kwp=panel_count_or_kwp,
        module_length=module_length,
        module_width=module_width,
        module_efficiency=module_eff,
        total_panels=total_panels,
        commissioning_date=comm_str,
        degradation_rate=degr,
        has_model_file=has_model,
        errors=errors,
    )


def list_topic_specs(limit: int = 200, like: str | None = None) -> list[dict[str, Any]]:
    sql = "SELECT tag FROM tag_specification WHERE tag IS NOT NULL"
    params: dict[str, Any] = {}
    if like:
        sql += " AND tag ILIKE :like"
        params["like"] = f"%{like}%"
    sql += " ORDER BY tag LIMIT :limit"
    params["limit"] = limit

    with engine_spec.connect() as conn:
        topics = [r[0] for r in conn.execute(text(sql), params).fetchall()]

    out = []
    for topic in topics:
        spec = get_topic_spec(topic)
        if not spec:
            continue
        out.append(
            {
                "topic": spec.topic,
                "user_object_id": spec.user_object_id,
                "lat": spec.lat,
                "lon": spec.lon,
                "tilt": spec.tilt,
                "azimuth": spec.azimuth,
                "panel_count_or_kwp": spec.panel_count_or_kwp,
                "has_model_file": spec.has_model_file,
                "errors": spec.errors,
            }
        )
    return out


def decide_weather_source(target_date: date, requested: str) -> tuple[str, str]:
    today = date.today()
    if requested in ("archive_db", "weather_api"):
        return requested, f"forced source={requested}"

    if target_date < today:
        return "archive_db", "historical date uses archive_db"
    return "weather_api", "today/future date uses weather_api"


def get_weather(topic: str, d: date, requested_source: str = "auto") -> dict[str, Any]:
    spec = get_topic_spec(topic)
    if not spec:
        raise HTTPException(status_code=404, detail="topic not found")

    source, reason = decide_weather_source(d, requested_source)
    points: list[dict[str, Any]]
    if source == "archive_db":
        if spec.user_object_id is None:
            raise HTTPException(
                status_code=400,
                detail={"weather_source_used": source, "weather_source_reason": reason + "; missing user_object_id"},
            )
        points = extract_weather_from_db(spec.user_object_id, d.strftime("%Y-%m-%d"))
    else:
        if spec.lat is None or spec.lon is None:
            raise HTTPException(
                status_code=400,
                detail={"weather_source_used": source, "weather_source_reason": reason + "; missing coords"},
            )
        points = get_forecast_by_coords(spec.lat, spec.lon, d)

    if not points:
        raise HTTPException(
            status_code=404,
            detail={"weather_source_used": source, "weather_source_reason": reason + "; no weather points returned"},
        )

    return {
        "topic": topic,
        "date": d.strftime("%Y-%m-%d"),
        "weather_source_used": source,
        "weather_source_reason": reason,
        "meta": {"user_object_id": spec.user_object_id, "lat": spec.lat, "lon": spec.lon},
        "points": points,
    }


def compute_forecast(topic: str, d: date, weather_source: str = "auto", write: bool = False) -> dict[str, Any]:
    t0 = time.perf_counter()
    spec = get_topic_spec(topic)
    if not spec:
        raise HTTPException(status_code=404, detail="topic not found")
    spec_ms = int((time.perf_counter() - t0) * 1000)

    t1 = time.perf_counter()
    weather = get_weather(topic, d, weather_source)
    weather_ms = int((time.perf_counter() - t1) * 1000)

    t2 = time.perf_counter()
    model_name = topic.replace("/", "_") + "_model.pkl"
    model = load_model(model_name)
    used_model = model_name if model is not None else "fallback_or_none"

    panel_area = ((spec.module_length or 1000) / 1000) * ((spec.module_width or 1000) / 1000)
    module_eff = (spec.module_efficiency or 17.7) / 100.0
    num_panels = max(spec.total_panels, 1)
    commissioning = datetime.strptime(spec.commissioning_date or d.strftime("%Y-%m-%d"), "%Y-%m-%d")

    out: list[dict[str, Any]] = []
    rows: list[tuple[str, datetime, float]] = []
    irr_ms = 0
    model_ms = 0
    for rec in weather["points"]:
        if "time" in rec:
            dt = datetime.strptime(rec["time"], "%Y-%m-%d %H:%M")
        else:
            dt = datetime.strptime(rec["x"], "%Y-%m-%d %H:%M")
        irr_t = time.perf_counter()
        irr = calculate_panel_irradiance(spec.lat or 0.0, spec.lon or 0.0, dt, spec.tilt, spec.azimuth, tz=TZ)
        irr_ms += int((time.perf_counter() - irr_t) * 1000)

        m_t = time.perf_counter()
        if irr < THRESHOLD_RADIATION:
            predicted = 0.0
        elif model is not None:
            df = pd.DataFrame(
                {"radiation_w_m2_y": [irr], "cloud": [float(rec.get("cloud", 0) or 0)]}
            )
            predicted = float(model.predict(df)[0])
        else:
            predicted = irr
        model_ms += int((time.perf_counter() - m_t) * 1000)

        base = predicted * panel_area * module_eff
        power = calculate_system_production(
            panel_power=base,
            temp_c=float(rec.get("temp_c", 25) or 25),
            cloud_cover=float(rec.get("cloud", 0) or 0) / 100.0,
            num_panels=num_panels,
            forecast_date=dt,
            commissioning_date=commissioning,
            degradation_rate=spec.degradation_rate,
        )
        out.append({"x": dt.strftime("%Y-%m-%d %H:%M"), "y": float(power)})
        rows.append((topic, dt, float(power)))

    cache_written = False
    if write and rows:
        ensure_month_partitions(rows[0][1], rows[-1][1])
        bulk_upsert_points(rows)
        cache_written = True

    total_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "topic": topic,
        "date": d.strftime("%Y-%m-%d"),
        "weather_source_used": weather["weather_source_used"],
        "weather_source_reason": weather["weather_source_reason"],
        "points": out,
        "meta": {
            "used_model": used_model,
            "timings_ms": {
                "spec": spec_ms,
                "weather": weather_ms,
                "irradiance": irr_ms,
                "model": model_ms,
                "total": total_ms,
            },
            "warnings": spec.errors,
            "cache_written": cache_written,
        },
    }


def start_history_job(
    topics: list[str],
    days_back: int,
    end_date: date,
    write: bool,
    max_days_per_run: int,
    max_topics_per_run: int,
) -> dict[str, Any]:
    accepted: list[str] = []
    skipped: list[dict[str, str]] = []
    for topic in topics[:max_topics_per_run]:
        spec = get_topic_spec(topic)
        if not spec:
            skipped.append({"topic": topic, "reason": "topic not found"})
            continue
        if spec.user_object_id is None:
            skipped.append({"topic": topic, "reason": "missing user_object_id"})
            continue
        accepted.append(topic)

    total_days = min(days_back, max_days_per_run)
    start_day = end_date - timedelta(days=total_days - 1)
    job_id = str(uuid.uuid4())
    state = {
        "job_id": job_id,
        "state": "running",
        "processed_days": 0,
        "total_days": total_days,
        "written_points": 0,
        "last_errors": [],
    }
    JOBS[job_id] = state

    try:
        rows = []
        for idx in range(total_days):
            d = start_day + timedelta(days=idx)
            for topic in accepted:
                result = compute_forecast(topic, d, weather_source="archive_db", write=False)
                for p in result["points"]:
                    ts = datetime.strptime(p["x"], "%Y-%m-%d %H:%M")
                    rows.append((topic, ts, float(p["y"])))
            state["processed_days"] = idx + 1

        if write and rows:
            ensure_month_partitions(rows[0][1], rows[-1][1])
            bulk_upsert_points(rows)
            state["written_points"] = len(rows)

        state["state"] = "done"
    except Exception as exc:
        state["state"] = "failed"
        state["last_errors"].append(str(exc))

    return {"job_id": job_id, "accepted_topics": accepted, "skipped_topics": skipped}


def get_job_status(job_id: str) -> dict[str, Any]:
    return JOBS.get(
        job_id,
        {
            "job_id": job_id,
            "state": "failed",
            "processed_days": 0,
            "total_days": 0,
            "written_points": 0,
            "last_errors": ["job not found"],
        },
    )


def precompute_future_all_topics() -> None:
    with engine_spec.connect() as conn:
        topics = [r[0] for r in conn.execute(text("SELECT tag FROM tag_specification WHERE tag IS NOT NULL")).fetchall()]

    start_day = date.today()
    end_day = start_day + timedelta(days=settings.forecast_days_ahead)
    rows: list[tuple[str, datetime, float]] = []

    cursor = start_day
    while cursor <= end_day:
        for topic in topics:
            try:
                result = compute_forecast(topic, cursor, weather_source="weather_api", write=False)
            except Exception:
                continue
            for p in result["points"]:
                ts = datetime.strptime(p["x"], "%Y-%m-%d %H:%M")
                rows.append((topic, ts, float(p["y"])))
        cursor += timedelta(days=1)

    if rows:
        ensure_month_partitions(rows[0][1], rows[-1][1])
        bulk_upsert_points(rows)
