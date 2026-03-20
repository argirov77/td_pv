import logging

import javaobj
import pandas as pd
from sqlalchemy import create_engine, text

from config import load_settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = load_settings()
engine = create_engine(settings.solar_db_dsn, pool_pre_ping=True)


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


def extract_weather_from_db(user_object_id, prediction_date):
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
        with engine.connect() as conn:
            result = conn.execute(query, {"user_object_id": user_object_id, "prediction_date": prediction_date}).fetchone()
    except Exception as exc:
        raise WeatherArchiveError(
            "postgres_query",
            f"Ошибка чтения weather_data из PostgreSQL: {exc}",
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

    df = pd.DataFrame(data)
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
        logger.warning("[extract_weather_from_db] Duplicate timestamps detected; aggregating before resample.")
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
