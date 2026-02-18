import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _int_from_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return int(value)


@dataclass(frozen=True)
class Settings:
    forecast_refresh_hours: int
    forecast_days_ahead: int
    forecast_history_days: int
    archive_db_dsn: str
    solar_db_dsn: str
    forecast_db_dsn: str
    weather_api_key: str
    model_version: str
    max_topics_per_request: int


def load_settings() -> Settings:
    return Settings(
        forecast_refresh_hours=_int_from_env("FORECAST_REFRESH_HOURS", default=3),
        forecast_days_ahead=_int_from_env("FORECAST_DAYS_AHEAD", default=7),
        forecast_history_days=_int_from_env("FORECAST_HISTORY_DAYS", default=365),
        archive_db_dsn=_required("ARCHIVE_DB_DSN"),
        solar_db_dsn=_required("SOLAR_DB_DSN"),
        forecast_db_dsn=_required("FORECAST_DB_DSN"),
        weather_api_key=_required("WEATHER_API_KEY"),
        model_version=_required("MODEL_VERSION"),
        max_topics_per_request=_int_from_env("MAX_TOPICS_PER_REQUEST", default=1000),
    )
