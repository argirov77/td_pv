import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


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
        forecast_refresh_hours=int(_required("FORECAST_REFRESH_HOURS")),
        forecast_days_ahead=int(_required("FORECAST_DAYS_AHEAD")),
        forecast_history_days=int(_required("FORECAST_HISTORY_DAYS")),
        archive_db_dsn=_required("ARCHIVE_DB_DSN"),
        solar_db_dsn=_required("SOLAR_DB_DSN"),
        forecast_db_dsn=_required("FORECAST_DB_DSN"),
        weather_api_key=_required("WEATHER_API_KEY"),
        model_version=_required("MODEL_VERSION"),
        max_topics_per_request=int(_required("MAX_TOPICS_PER_REQUEST")),
    )
