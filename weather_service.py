from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Literal, TypedDict

from weather_api import get_forecast_by_coords
from weather_db import WeatherArchiveError, extract_weather_from_db, get_weather_by_replicator_id

logger = logging.getLogger(__name__)


class WeatherFetchResult(TypedDict):
    records: list[dict]
    source: Literal["archive_db_new", "archive_db", "weather_api", "none"]
    status: Literal["ok", "no_data"]
    diagnostics: dict[str, str] | None


def _weather_non_null_points(records: list[dict]) -> int:
    """Count points where at least one core weather field is present."""
    return sum(1 for rec in records if rec.get("temp_c") is not None or rec.get("cloud") is not None)


def get_weather_for_date(
    *,
    replicator_id: str | None = None,
    user_object_id: int,
    latitude: float,
    longitude: float,
    prediction_date: date,
) -> WeatherFetchResult:
    today = datetime.utcnow().date()

    diagnostics: dict[str, str] = {}

    logger.info(
        "[weather] date=%s replicator_id=%s user_object_id=%s",
        prediction_date, replicator_id, user_object_id,
    )

    def _load(source: str, loader):
        try:
            recs = loader() or []
        except WeatherArchiveError as exc:
            diagnostics[f"{source}_stage"] = exc.stage
            diagnostics[f"{source}_error"] = str(exc)
            logger.warning("[weather] %s failed at stage=%s: %s", source, exc.stage, exc)
            return []
        except Exception as exc:
            diagnostics[f"{source}_stage"] = "unexpected"
            diagnostics[f"{source}_error"] = str(exc)
            logger.warning("[weather] %s failed unexpectedly: %s", source, exc)
            return []
        if recs:
            logger.info("[weather] %s returned %d records for date=%s", source, len(recs), prediction_date)
        else:
            logger.info("[weather] %s returned empty result for date=%s", source, prediction_date)
        return recs

    def _try_source(source: str, records: list[dict]) -> WeatherFetchResult | None:
        non_null = _weather_non_null_points(records)
        if records:
            diagnostics[f"{source}_records"] = str(len(records))
            diagnostics[f"{source}_non_null_points"] = str(non_null)
        if records and non_null > 0:
            return {"records": records, "source": source, "status": "ok", "diagnostics": diagnostics or None}
        if records:
            diagnostics[f"{source}_stage"] = "empty_weather_values"
            diagnostics[f"{source}_error"] = "records are present, but temp_c/cloud are null for all points"
        return None

    # Build ordered source list
    sources: list[tuple[str, object]] = []

    if prediction_date < today:
        # Historical: new DB first, then old DB, then API
        if replicator_id is not None:
            sources.append(("archive_db_new", lambda: get_weather_by_replicator_id(replicator_id, prediction_date.strftime("%Y-%m-%d"))))
        sources.append(("archive_db", lambda: extract_weather_from_db(user_object_id, prediction_date.strftime("%Y-%m-%d"))))
        sources.append(("weather_api", lambda: get_forecast_by_coords(latitude, longitude, prediction_date)))
    else:
        # Future: API first, then new DB, then old DB
        sources.append(("weather_api", lambda: get_forecast_by_coords(latitude, longitude, prediction_date)))
        if replicator_id is not None:
            sources.append(("archive_db_new", lambda: get_weather_by_replicator_id(replicator_id, prediction_date.strftime("%Y-%m-%d"))))
        sources.append(("archive_db", lambda: extract_weather_from_db(user_object_id, prediction_date.strftime("%Y-%m-%d"))))

    logger.info("[weather] sources=%s", [s[0] for s in sources])

    for source_name, loader in sources:
        records = _load(source_name, loader)
        result = _try_source(source_name, records)
        if result is not None:
            return result

    return {"records": [], "source": "none", "status": "no_data", "diagnostics": diagnostics or None}
