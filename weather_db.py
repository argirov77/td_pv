import pandas as pd
from sqlalchemy import create_engine, text
import javaobj
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --- Параметри за връзка ---
DB_URI = "postgresql://postgres:smartgrid@172.31.168.2/solar_db"
engine = create_engine(DB_URI)

def deserialize_java_object(binary_value):
    """
    Десериализира бинарната стойност (bytea) с помощта на javaobj.loads.
    Ако стойността е None или възникне грешка при десериализацията, връща None.
    """
    if binary_value is None:
        return None
    if isinstance(binary_value, memoryview):
        binary_value = binary_value.tobytes()
    try:
        return javaobj.loads(binary_value)
    except Exception as e:
        logger.error("Грешка при десериализация на Java обект: %s", e)
        return None

def unwrap_value(obj):
    """
    Ако obj има атрибут .value (JavaDouble/JavaInteger/JavaString) — връща obj.value,
    иначе връща obj непроменен.
    """
    return getattr(obj, "value", obj)

def extract_forecast_data(forecast_obj):
    """
    Извлича от forecast_obj списък записи:
      - time   (низ във формат "YYYY-MM-DD HH:MM")
      - temp_c (float)
      - cloud  (int)
    Разопакова Java обвивки в Python типове.
    """
    days = getattr(forecast_obj, "forecastday", None)
    if days is None:
        return []
    try:
        days = list(days)
    except Exception as e:
        logger.error("Неуспешно преобразуване на forecastday в списък: %s", e)
        return []

    data = []
    for day in days:
        hours = getattr(day, "hour", None)
        if hours is None:
            continue
        try:
            hours = list(hours)
        except Exception as e:
            logger.error("Неуспешно преобразуване на hour в списък: %s", e)
            continue

        for hour in hours:
            # сурови атрибути
            t_raw     = getattr(hour, "time", None)
            temp_raw  = getattr(hour, "tempC", None)      # JavaDouble
            cloud_raw = getattr(hour, "cloud", None)      # JavaInteger

            # разопаковане
            t_str     = unwrap_value(t_raw)
            temp_val  = unwrap_value(temp_raw)
            cloud_val = unwrap_value(cloud_raw)

            # кастване
            try:
                temp_c = float(temp_val) if temp_val is not None else None
            except Exception:
                temp_c = None
            try:
                cloud = int(cloud_val) if cloud_val is not None else None
            except Exception:
                cloud = None

            if t_str:
                data.append({
                    "time": str(t_str),
                    "temp_c": temp_c,
                    "cloud": cloud
                })

    return data

def extract_weather_from_db(user_object_id, prediction_date):
    """
    Основен поток:
      1) Извличане на current_data от weather_data
      2) Десериализация на Java обект
      3) Извличане на forecast → extract_forecast_data
      4) Създаване на DataFrame, конвертиране на time в datetime, премахване на дубликати
      5) Привеждане на temp_c и cloud към числови типове
      6) Ресемплиране на 15-минутен интервал + интерполация
    """
    sql = text("""
        SELECT current_data
        FROM weather_data
        WHERE user_object_id = :uid
          AND date = :dt
        ORDER BY id
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"uid": user_object_id, "dt": prediction_date}).fetchone()

    if not row:
        logger.error("Няма записи за user_object_id=%s на %s", user_object_id, prediction_date)
        return None

    root_obj = deserialize_java_object(row[0])
    if root_obj is None:
        return None

    forecast_obj = getattr(root_obj, "forecast", None)
    if forecast_obj is None:
        logger.error("В десериализирания обект няма атрибут forecast")
        return None

    raw = extract_forecast_data(forecast_obj)
    if not raw:
        logger.error("extract_forecast_data върна празен списък")
        return None

    df = pd.DataFrame(raw)

    # конвертиране на време и индексиране
    try:
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M")
    except Exception as e:
        logger.error("Неуспешна конверсия на време: %s", e)
        return None

    df = df.drop_duplicates(subset="time").set_index("time").sort_index()

    # числови колони
    for col in ["temp_c", "cloud"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # проверка дали има какво да се интерполира
    if df[["temp_c", "cloud"]].isna().all().all():
        logger.error("Няма валидни числа в temp_c или cloud за интерполация")
        return None

    # ресемплиране + интерполация
    try:
        df15 = df.resample("15min").interpolate(method="linear")
    except Exception as e:
        logger.error("Грешка при ресемплиране: %s", e)
        return None

    # финална подготовка
    df15 = df15.reset_index()
    df15["time"] = df15["time"].dt.strftime("%Y-%m-%d %H:%M")
    return df15.to_dict(orient="records")

if __name__ == "__main__":
    uid    = 70
    date   = "2025-03-20"
    outcsv = "cloud_chronology_15.csv"

    records = extract_weather_from_db(uid, date)
    if records:
        df_out = pd.DataFrame(records)
        df_out.to_csv(outcsv, index=False)
        print(f"Успешно записано във {outcsv}")
        print(df_out.head())
    else:
        print("Неуспя да получи данни.")
