from __future__ import annotations

from datetime import date, datetime
from typing import Literal, TypedDict

from weather_api import get_forecast_by_coords
from weather_db import WeatherArchiveError, extract_weather_from_db


class WeatherFetchResult(TypedDict):
    records: list[dict]
    source: Literal["archive_db", "weather_api", "none"]
    status: Literal["ok", "no_data"]
    diagnostics: dict[str, str] | None


def _weather_non_null_points(records: list[dict]) -> int:
    """Count points where at least one core weather field is present."""
    return sum(1 for rec in records if rec.get("temp_c") is not None or rec.get("cloud") is not None)


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

    diagnostics: dict[str, str] = {}

    def _load(source: str, loader):
        try:
            return loader() or []
        except WeatherArchiveError as exc:
            diagnostics[f"{source}_stage"] = exc.stage
            diagnostics[f"{source}_error"] = str(exc)
            return []
        except Exception as exc:
            diagnostics[f"{source}_stage"] = "unexpected"
            diagnostics[f"{source}_error"] = str(exc)
            return []

    if prediction_date < today:
        primary = ("archive_db", lambda: extract_weather_from_db(user_object_id, prediction_date.strftime("%Y-%m-%d")))
        secondary = ("weather_api", lambda: get_forecast_by_coords(latitude, longitude, prediction_date))
    else:
        primary = ("weather_api", lambda: get_forecast_by_coords(latitude, longitude, prediction_date))
        secondary = ("archive_db", lambda: extract_weather_from_db(user_object_id, prediction_date.strftime("%Y-%m-%d")))

    primary_source, primary_loader = primary
    records = _load(primary_source, primary_loader)
    primary_non_null_points = _weather_non_null_points(records)
    if records:
        diagnostics[f"{primary_source}_records"] = str(len(records))
        diagnostics[f"{primary_source}_non_null_points"] = str(primary_non_null_points)
    if records and primary_non_null_points > 0:
        return {"records": records, "source": primary_source, "status": "ok", "diagnostics": diagnostics or None}
    if records:
        diagnostics[f"{primary_source}_stage"] = "empty_weather_values"
        diagnostics[f"{primary_source}_error"] = "records are present, but temp_c/cloud are null for all points"

    secondary_source, secondary_loader = secondary
    records = _load(secondary_source, secondary_loader)
    secondary_non_null_points = _weather_non_null_points(records)
    if records:
        diagnostics[f"{secondary_source}_records"] = str(len(records))
        diagnostics[f"{secondary_source}_non_null_points"] = str(secondary_non_null_points)
    if records and secondary_non_null_points > 0:
        return {"records": records, "source": secondary_source, "status": "ok", "diagnostics": diagnostics or None}
    if records:
        diagnostics[f"{secondary_source}_stage"] = "empty_weather_values"
        diagnostics[f"{secondary_source}_error"] = "records are present, but temp_c/cloud are null for all points"

    return {"records": [], "source": "none", "status": "no_data", "diagnostics": diagnostics or None}
