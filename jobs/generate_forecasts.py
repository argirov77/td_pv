import argparse
import logging
from datetime import datetime, timedelta

import pandas as pd

from config import load_settings
from database import get_all_topics, get_tag_specification
from forecast_db import bulk_upsert_points, delete_future, ensure_month_partitions, find_missing_days, run_migrations
from model_loader import load_model
from production import calculate_system_production
from radiation import calculate_panel_irradiance
from weather_service import get_weather_for_date

logger = logging.getLogger(__name__)

THRESHOLD_RADIATION = 40
settings = load_settings()


def _build_rows_for_topic(topic: str, records: list[dict], source: str) -> list[tuple[str, datetime, float, str]]:
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
        out.append((topic, dt, float(power), source))
    return out


def run_future() -> None:
    now = datetime.utcnow()
    end = now + timedelta(days=settings.forecast_days_ahead)
    run_migrations()
    ensure_month_partitions(now, end)
    delete_future(now)

    rows = []
    for topic in get_all_topics():
        spec = get_tag_specification(topic)
        if not spec:
            continue
        uid = spec.get("sm_user_object_id")
        lat = spec.get("latitude")
        lon = spec.get("longitude")
        if uid is None or lat is None or lon is None:
            continue

        rid = spec.get("replicator_id")
        for day_offset in range(settings.forecast_days_ahead + 1):
            target_day = (now + timedelta(days=day_offset)).date()
            weather_result = get_weather_for_date(
                replicator_id=rid,
                user_object_id=int(uid),
                latitude=float(lat),
                longitude=float(lon),
                prediction_date=target_day,
            )
            rows.extend(_build_rows_for_topic(topic, weather_result["records"], weather_result["source"]))
            if len(rows) >= 5000:
                bulk_upsert_points(rows)
                rows.clear()

    if rows:
        bulk_upsert_points(rows)



def run_history(days: int | None = None) -> None:
    history_days = settings.forecast_history_days if days is None else days
    now = datetime.utcnow()
    start = now - timedelta(days=history_days)
    run_migrations()
    ensure_month_partitions(start, now)

    start_date = start.date()
    end_date = now.date()

    rows: list[tuple] = []
    for topic in get_all_topics():
        spec = get_tag_specification(topic)
        if not spec:
            continue
        uid = spec.get("sm_user_object_id")
        lat = spec.get("latitude")
        lon = spec.get("longitude")
        if uid is None or lat is None or lon is None:
            continue

        missing = find_missing_days(topic, start_date, end_date)
        if not missing:
            logger.debug("topic %s: no gaps found", topic)
            continue

        logger.info("topic %s: filling %d missing days", topic, len(missing))
        rid = spec.get("replicator_id")
        for day in missing:
            weather_result = get_weather_for_date(
                replicator_id=rid,
                user_object_id=int(uid),
                latitude=float(lat),
                longitude=float(lon),
                prediction_date=day,
            )
            rows.extend(_build_rows_for_topic(topic, weather_result["records"], weather_result["source"]))
            if len(rows) >= 5000:
                bulk_upsert_points(rows)
                rows.clear()

    if rows:
        bulk_upsert_points(rows)


def run_fixation() -> None:
    now = datetime.utcnow()
    yesterday = (now - timedelta(days=1)).date()
    run_migrations()
    ensure_month_partitions(datetime.combine(yesterday, datetime.min.time()), now)

    rows: list[tuple] = []
    fixed = 0
    skipped = 0
    for topic in get_all_topics():
        spec = get_tag_specification(topic)
        if not spec:
            continue
        uid = spec.get("sm_user_object_id")
        lat = spec.get("latitude")
        lon = spec.get("longitude")
        if uid is None or lat is None or lon is None:
            continue

        rid = spec.get("replicator_id")
        weather_result = get_weather_for_date(
            replicator_id=rid,
            user_object_id=int(uid),
            latitude=float(lat),
            longitude=float(lon),
            prediction_date=yesterday,
        )
        source = weather_result.get("source", "none")
        if source == "weather_api" or source == "none":
            skipped += 1
            continue

        rows.extend(_build_rows_for_topic(topic, weather_result["records"], "archive_db"))
        fixed += 1
        if len(rows) >= 5000:
            bulk_upsert_points(rows)
            rows.clear()

    if rows:
        bulk_upsert_points(rows)

    logger.info("fixation for %s: fixed=%d, skipped=%d", yesterday, fixed, skipped)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["future", "history", "fixation"], required=True)
    parser.add_argument("--days", type=int, default=None)
    args = parser.parse_args()

    if args.mode == "future":
        run_future()
    elif args.mode == "fixation":
        run_fixation()
    else:
        run_history(days=args.days)


if __name__ == "__main__":
    main()
