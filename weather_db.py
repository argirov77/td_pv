import logging

import javaobj
import pandas as pd
from sqlalchemy import create_engine, text

from config import load_settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = load_settings()
engine = create_engine(settings.solar_db_dsn, pool_pre_ping=True)


def deserialize_java_object(binary_value):
    if binary_value is None:
        return None
    if isinstance(binary_value, memoryview):
        binary_value = binary_value.tobytes()
    try:
        return javaobj.loads(binary_value)
    except Exception:
        return None


def extract_forecast_data(forecast_obj):
    days = getattr(forecast_obj, "forecastday", None)
    if days is None:
        return []
    try:
        days = list(days)
    except Exception:
        return []

    data = []
    for day in days:
        hours = getattr(day, "hour", None)
        if hours is None:
            continue
        try:
            hours = list(hours)
        except Exception:
            continue
        for hour in hours:
            rec_time = getattr(hour, "time", None)
            if rec_time is None:
                continue
            data.append({
                "time": str(rec_time),
                "temp_c": getattr(hour, "temp_c", None),
                "cloud": getattr(hour, "cloud", None),
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

    with engine.connect() as conn:
        result = conn.execute(query, {"user_object_id": user_object_id, "prediction_date": prediction_date}).fetchone()

    if not result:
        return []

    current_data = deserialize_java_object(result[0])
    if current_data is None:
        return []

    forecast_obj = getattr(current_data, "forecast", None)
    if forecast_obj is None:
        return []

    data = extract_forecast_data(forecast_obj)
    if not data:
        return []

    df = pd.DataFrame(data)
    try:
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M")
    except Exception:
        return []

    df["temp_c"] = pd.to_numeric(df["temp_c"], errors="coerce")
    df["cloud"] = pd.to_numeric(df["cloud"], errors="coerce")

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
    df_15min["time"] = df_15min["time"].dt.strftime("%Y-%m-%d %H:%M")
    return df_15min.to_dict(orient="records")
