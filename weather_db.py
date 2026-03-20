import logging

import javaobj
import pandas as pd
from sqlalchemy import create_engine, text

from config import load_settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = load_settings()

# Old server (solar_db on 172.31.168.2) — fallback
engine = create_engine(settings.solar_db_dsn, pool_pre_ping=True)

# New server (weather_main2 on 172.31.168.3) — primary
engine_weather_main = create_engine(settings.weather_db_dsn, pool_pre_ping=True)


class WeatherArchiveError(Exception):
    def __init__(self, stage: str, message: str, *, original_exception: Exception | None = None):
        super().__init__(message)
        self.stage = stage
        self.original_exception = original_exception


def deserialize_java_object(binary_value):
    if binary_value is None:
        return None
    if isinstance(binary_value, memoryview):
        binary_value = binary_value.tobytes()
    if isinstance(binary_value, bytes) and len(binary_value) == 0:
        raise WeatherArchiveError("java_deserialization", "Получен пустой бинарный payload из PostgreSQL")
    try:
        return javaobj.loads(binary_value)
    except Exception as exc:
        raise WeatherArchiveError(
            "java_deserialization",
            f"Не удалось десериализовать Java payload: {exc}",
            original_exception=exc,
        ) from exc


def unwrap_value(obj):
    """
    If Java wrapper object exposes `.value`, return the wrapped primitive value.
    """
    return getattr(obj, "value", obj)


def extract_forecast_data(forecast_obj):
    days = getattr(forecast_obj, "forecastday", None)
    if days is None:
        raise WeatherArchiveError("java_object_parse", "В Java-объекте отсутствует forecastday")
    try:
        days = list(days)
    except Exception as exc:
        raise WeatherArchiveError(
            "java_object_parse",
            f"Поле forecastday не итерируемо: {exc}",
            original_exception=exc,
        ) from exc

    data = []
    for day in days:
        hours = getattr(day, "hour", None)
        if hours is None:
            continue
        try:
            hours = list(hours)
        except Exception as exc:
            raise WeatherArchiveError(
                "java_object_parse",
                f"Поле hour не итерируемо: {exc}",
                original_exception=exc,
            ) from exc
        for hour in hours:
            rec_time = unwrap_value(getattr(hour, "time", None))
            if rec_time is None:
                continue

            # Java payloads may expose camelCase fields and wrapper objects.
            temp_raw = getattr(hour, "temp_c", None)
            if temp_raw is None:
                temp_raw = getattr(hour, "tempC", None)

            cloud_raw = getattr(hour, "cloud", None)

            data.append({
                "time": str(rec_time),
                "temp_c": unwrap_value(temp_raw),
                "cloud": unwrap_value(cloud_raw),
            })
    return data


def _process_weather_dataframe(df: pd.DataFrame, source_label: str) -> list[dict]:
    """Common processing: parse time, resample to 15min, validate."""
    try:
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M")
    except Exception as exc:
        raise WeatherArchiveError(
            "weather_timeseries_parse",
            f"Не удалось распарсить time в исторической погоде: {exc}",
            original_exception=exc,
        ) from exc

    if "temp_c" not in df.columns or "cloud" not in df.columns:
        raise WeatherArchiveError(
            "weather_fields_missing",
            "В исторической погоде отсутствуют ожидаемые поля temp_c/cloud",
        )

    df["temp_c"] = pd.to_numeric(df["temp_c"], errors="coerce")
    df["cloud"] = pd.to_numeric(df["cloud"], errors="coerce")

    if df[["temp_c", "cloud"]].notna().sum().sum() == 0:
        raise WeatherArchiveError(
            "weather_values_empty",
            "Историческая погода прочитана, но temp_c и cloud пустые во всех исходных точках",
        )

    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)
    if df.index.has_duplicates:
        logger.warning("[%s] Duplicate timestamps detected; aggregating before resample.", source_label)
        df = df.groupby(level=0).mean(numeric_only=True)

    numeric_cols = df.select_dtypes(include="number").columns
    if numeric_cols.empty:
        return []

    df_15min = (
        df[numeric_cols]
        .resample("15min")
        .interpolate(method="linear")
        .reset_index()
    )
    if "cloud" in df_15min.columns:
        df_15min["cloud"] = df_15min["cloud"].round().astype("Int64")

    if df_15min[["temp_c", "cloud"]].notna().sum().sum() == 0:
        raise WeatherArchiveError(
            "weather_values_empty_after_resample",
            "После ресемплинга историческая погода содержит только пустые temp_c/cloud",
        )

    df_15min["time"] = df_15min["time"].dt.strftime("%Y-%m-%d %H:%M")
    return df_15min.to_dict(orient="records")


def _fetch_and_parse_weather(db_engine, user_object_id, prediction_date, source_label: str) -> list[dict]:
    """Fetch current_data from weather_data table, deserialize Java, process to records."""
    query = text(
        """
        SELECT current_data
        FROM weather_data
        WHERE user_object_id = :user_object_id
          AND date::date = :prediction_date
        ORDER BY date ASC
        LIMIT 1
        """
    )

    try:
        with db_engine.connect() as conn:
            result = conn.execute(query, {"user_object_id": user_object_id, "prediction_date": prediction_date}).fetchone()
    except Exception as exc:
        raise WeatherArchiveError(
            "postgres_query",
            f"Ошибка чтения weather_data ({source_label}): {exc}",
            original_exception=exc,
        ) from exc

    if not result:
        return []

    current_data = deserialize_java_object(result[0])
    if current_data is None:
        return []

    forecast_obj = getattr(current_data, "forecast", None)
    if forecast_obj is None:
        raise WeatherArchiveError("java_object_parse", "В Java-объекте отсутствует поле forecast")

    data = extract_forecast_data(forecast_obj)
    if not data:
        return []

    return _process_weather_dataframe(pd.DataFrame(data), source_label)


# ---------------------------------------------------------------------------
# Old path: solar_db on 172.31.168.2 (sm_user_object_id -> weather_data)
# ---------------------------------------------------------------------------

def extract_weather_from_db(user_object_id, prediction_date):
    """Old path: fetch weather from solar_db by sm_user_object_id."""
    return _fetch_and_parse_weather(engine, user_object_id, prediction_date, "solar_db")


# ---------------------------------------------------------------------------
# New path: weather_main2 on 172.31.168.3 (replicator_id -> user_objects -> weather_data)
# ---------------------------------------------------------------------------

_user_object_cache: dict[int, int] = {}


def resolve_user_object_id(replicator_id: int) -> int | None:
    """Look up user_object_id from user_objects table in weather_main2 by replicator_id."""
    if replicator_id in _user_object_cache:
        return _user_object_cache[replicator_id]

    query = text(
        """
        SELECT user_object_id
        FROM user_objects
        WHERE replicator_id = :replicator_id
        LIMIT 1
        """
    )
    try:
        with engine_weather_main.connect() as conn:
            result = conn.execute(query, {"replicator_id": replicator_id}).fetchone()
    except Exception as exc:
        raise WeatherArchiveError(
            "replicator_id_lookup",
            f"Ошибка поиска user_object_id по replicator_id={replicator_id}: {exc}",
            original_exception=exc,
        ) from exc

    if not result:
        logger.warning("[weather_main2] No user_object_id found for replicator_id=%s", replicator_id)
        return None

    user_object_id = result[0]
    _user_object_cache[replicator_id] = user_object_id
    logger.info("[weather_main2] Resolved replicator_id=%s -> user_object_id=%s", replicator_id, user_object_id)
    return user_object_id


def extract_weather_from_new_db(user_object_id: int, prediction_date: str) -> list[dict]:
    """New path: fetch weather from weather_main2 by user_object_id."""
    return _fetch_and_parse_weather(engine_weather_main, user_object_id, prediction_date, "weather_main2")


def get_weather_by_replicator_id(replicator_id: int, prediction_date: str) -> list[dict]:
    """Resolve replicator_id -> user_object_id via user_objects, then extract weather from weather_main2."""
    user_object_id = resolve_user_object_id(replicator_id)
    if user_object_id is None:
        raise WeatherArchiveError(
            "replicator_id_lookup",
            f"Не найден user_object_id для replicator_id={replicator_id}",
        )
    return extract_weather_from_new_db(user_object_id, prediction_date)
