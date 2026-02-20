from __future__ import annotations

from datetime import date, datetime
from typing import Literal, TypedDict

from weather_api import get_forecast_by_coords
from weather_db import extract_weather_from_db


class WeatherFetchResult(TypedDict):
    records: list[dict]
    source: Literal["archive_db", "weather_api", "none"]
    status: Literal["ok", "no_data"]


def get_weather_for_date(
    *,
    user_object_id: int,
    latitude: float,
    longitude: float,
    prediction_date: date,
) -> WeatherFetchResult:
    """
    Единна точка за взимане на метео данни.

    Правила:
    - исторически дати (prediction_date < днес) -> archive_db;
    - прогнозни дати (prediction_date >= днес) -> weather_api.

    Ако предпочитаният източник няма данни, прави fallback към другия източник,
    за да покрие edge-case-а "няма данни никъде" с диагностика.
    """
    today = datetime.utcnow().date()

    if prediction_date < today:
        primary = ("archive_db", lambda: extract_weather_from_db(user_object_id, prediction_date.strftime("%Y-%m-%d")))
        secondary = ("weather_api", lambda: get_forecast_by_coords(latitude, longitude, prediction_date))
    else:
        primary = ("weather_api", lambda: get_forecast_by_coords(latitude, longitude, prediction_date))
        secondary = ("archive_db", lambda: extract_weather_from_db(user_object_id, prediction_date.strftime("%Y-%m-%d")))

    primary_source, primary_loader = primary
    records = primary_loader() or []
    if records:
        return {"records": records, "source": primary_source, "status": "ok"}

    secondary_source, secondary_loader = secondary
    records = secondary_loader() or []
    if records:
        return {"records": records, "source": secondary_source, "status": "ok"}

    return {"records": [], "source": "none", "status": "no_data"}
