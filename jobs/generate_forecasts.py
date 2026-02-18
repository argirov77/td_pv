import argparse
from datetime import datetime, timedelta

import pandas as pd

from config import load_settings
from database import get_all_topics, get_tag_specification
from forecast_db import bulk_upsert_points, delete_future, ensure_month_partitions, run_migrations
from model_loader import load_model
from production import calculate_system_production
from radiation import calculate_panel_irradiance
from weather_api import get_forecast_by_coords
from weather_db import extract_weather_from_db

THRESHOLD_RADIATION = 40
settings = load_settings()


def _build_rows_for_topic(topic: str, records: list[dict]) -> list[tuple[str, datetime, float]]:
    spec = get_tag_specification(topic)
    if not spec:
        return []

    uid = spec.get("sm_user_object_id")
    lat = spec.get("latitude")
    lon = spec.get("longitude")
    if uid is None or lat is None or lon is None:
        return []

    tilt = spec.get("tilt", 0.0)
    azimuth = spec.get("azimuth", 180.0)
    mlen = spec.get("module_length")
    mwid = spec.get("module_width")
    meff_pct = spec.get("module_efficiency", 17.7)
    panels = spec.get("total_panels", 0)
    comm = spec.get("commissioning_date")
    degr = spec.get("degradation_rate", 0.0)
    if not mlen or not mwid or not panels or not comm:
        return []

    panel_area = (mlen / 1000) * (mwid / 1000)
    mod_eff = meff_pct / 100.0
    model_name = topic.replace("/", "_") + "_model.pkl"
    model = load_model(model_name)

    out = []
    for rec in records:
        t = rec.get("time")
        if not t:
            continue
        dt = datetime.strptime(t, "%Y-%m-%d %H:%M")
        irr = calculate_panel_irradiance(lat, lon, dt, tilt, azimuth, tz="Europe/Nicosia")
        if irr < THRESHOLD_RADIATION:
            eff = 0.0
        elif model is not None:
            df = pd.DataFrame({"radiation_w_m2_y": [irr], "cloud": [float(rec.get("cloud", 0) or 0)]})
            eff = float(model.predict(df)[0])
        else:
            eff = irr

        base = eff * panel_area * mod_eff
        power = calculate_system_production(
            panel_power=base,
            temp_c=float(rec.get("temp_c", 25) or 25),
            cloud_cover=float(rec.get("cloud", 0) or 0) / 100.0,
            num_panels=int(panels),
            forecast_date=dt,
            commissioning_date=datetime.strptime(str(comm), "%Y-%m-%d"),
            degradation_rate=degr,
        )
        out.append((topic, dt, float(power)))
    return out


def run_future() -> None:
    now = datetime.utcnow()
    end = now + timedelta(days=settings.forecast_days_ahead)
    run_migrations()
    ensure_month_partitions(now, end)
    delete_future(now)

    rows = []
    for topic in get_all_topics():
        for day_offset in range(settings.forecast_days_ahead + 1):
            target_day = (now + timedelta(days=day_offset)).date()
            records = get_forecast_by_coords(*_topic_coords(topic), target_day)
            rows.extend(_build_rows_for_topic(topic, records))
            if len(rows) >= 5000:
                bulk_upsert_points(rows)
                rows.clear()

    if rows:
        bulk_upsert_points(rows)


def _topic_coords(topic: str) -> tuple[float, float]:
    spec = get_tag_specification(topic)
    if not spec:
        return 0.0, 0.0
    return float(spec.get("latitude", 0.0)), float(spec.get("longitude", 0.0))


def run_history() -> None:
    now = datetime.utcnow()
    start = now - timedelta(days=settings.forecast_history_days)
    run_migrations()
    ensure_month_partitions(start, now)

    rows = []
    for topic in get_all_topics():
        spec = get_tag_specification(topic)
        if not spec:
            continue
        uid = spec.get("sm_user_object_id")
        if uid is None:
            continue

        for day_offset in range(settings.forecast_history_days + 1):
            day = (start + timedelta(days=day_offset)).date()
            records = extract_weather_from_db(uid, day.strftime("%Y-%m-%d"))
            rows.extend(_build_rows_for_topic(topic, records))
            if len(rows) >= 5000:
                bulk_upsert_points(rows)
                rows.clear()

    if rows:
        bulk_upsert_points(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["future", "history"], required=True)
    args = parser.parse_args()

    if args.mode == "future":
        run_future()
    else:
        run_history()


if __name__ == "__main__":
    main()
