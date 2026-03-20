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
    """
    Единна точка за взимане на метео данни.

    Правила:
    - исторически дати (prediction_date < днес):
        1) archive_db_new (weather_main2 чрез replicator_id) — ако има replicator_id
        2) archive_db (solar_db чрез sm_user_object_id) — fallback
        3) weather_api — последен fallback
    - прогнозни дати (prediction_date >= днес):
        1) weather_api
        2) archive_db_new — ако има replicator_id
        3) archive_db — fallback
    """
    today = datetime.utcnow().date()

    diagnostics: dict[str, str] = {}

    def _load(source: str, loader):
        try:
            result = loader() or []
            if not result:
                logger.info("[weather] %s returned empty result for date=%s", source, prediction_date)
            else:
                logger.info("[weather] %s returned %d records for date=%s", source, len(result), prediction_date)
            return result
        except WeatherArchiveError as exc:
            logger.warning("[weather] %s failed at stage=%s: %s", source, exc.stage, exc)
            diagnostics[f"{source}_stage"] = exc.stage
            diagnostics[f"{source}_error"] = str(exc)
            return []
        except Exception as exc:
            logger.warning("[weather] %s unexpected error: %s", source, exc)
            diagnostics[f"{source}_stage"] = "unexpected"
            diagnostics[f"{source}_error"] = str(exc)
            return []

    def _try_source(source: str, loader) -> WeatherFetchResult | None:
        records = _load(source, loader)
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

    date_str = prediction_date.strftime("%Y-%m-%d")

    # Build ordered list of sources to try
    archive_sources: list[tuple[str, object]] = []
    if replicator_id is not None:
        archive_sources.append(("archive_db_new", lambda: get_weather_by_replicator_id(replicator_id, date_str)))
    archive_sources.append(("archive_db", lambda: extract_weather_from_db(user_object_id, date_str)))
    api_source = ("weather_api", lambda: get_forecast_by_coords(latitude, longitude, prediction_date))

    if prediction_date < today:
        sources = archive_sources + [api_source]
    else:
        sources = [api_source] + archive_sources

    logger.info("[weather] date=%s replicator_id=%s user_object_id=%s sources=%s",
                prediction_date, replicator_id, user_object_id, [s[0] for s in sources])

    for source_name, loader in sources:
        result = _try_source(source_name, loader)
        if result is not None:
            return result

    return {"records": [], "source": "none", "status": "no_data", "diagnostics": diagnostics or None}
