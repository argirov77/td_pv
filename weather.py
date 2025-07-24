from sqlalchemy import create_engine, text
import pandas as pd
import javaobj

# Свързване към базата solar_main2
DB_SOLAR_MAIN2 = "postgresql://postgres:smartgrid@10.4.4.123/solar_main2"
engine_weather = create_engine(DB_SOLAR_MAIN2)


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
        # Можете да добавите логиране на грешката: print("Грешка при десериализация:", e)
        return None


def extract_forecast_data(forecast_obj):
    """
    Извлича прогнозните данни от десериализирания обект forecast.
    За всеки ден (forecastday) и за всеки час (hour) се извличат:
      - time: време като низ
      - temp_c: температура (°C)
      - cloud: облачност (в проценти)
      - wind_kph: скорост на вятъра (км/ч)
    Връща списък от речници с тези полета.
    """
    forecast_days = getattr(forecast_obj, "forecastday", None)
    if forecast_days is None:
        return []
    try:
        forecast_days = list(forecast_days)
    except Exception:
        return []

    data = []
    for day in forecast_days:
        hours = getattr(day, "hour", None)
        if hours is None:
            continue
        try:
            hours = list(hours)
        except Exception:
            continue
        for hour in hours:
            rec_time = getattr(hour, "time", None)
            rec_temp = getattr(hour, "temp_c", None)
            rec_cloud = getattr(hour, "cloud", None)
            rec_wind = getattr(hour, "wind_kph", None)
            if rec_time is not None:
                data.append(
                    {
                        "time": str(rec_time),
                        "temp_c": rec_temp,
                        "cloud": rec_cloud,
                        "wind_kph": rec_wind,
                    }
                )
    return data


def extract_weather_from_db(user_object_id, prediction_date):
    """
    Извлича метеорологичните данни от таблицата weather_data
    за даден user_object_id и дата.

    Функцията изпълнява следните стъпки:
      1. Изпълнява се SQL заявка за получаване на записа (полето current_data);
      2. Десериализира се бинарното поле current_data;
      3. От обекта forecast се извлича списък с данни (time, temp_c, cloud, wind_kph);
      4. Данните се преобразуват в DataFrame, колоната time се конвертира в datetime,
         задава се като индекс, сортира се и се извършва ресемплиране
         до 15-минутен интервал.

    Ако на някой етап няма данни или възникне грешка – връща None.
    """
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
    with engine_weather.connect() as conn:
        result = conn.execute(
            query,
            {"user_object_id": user_object_id, "prediction_date": prediction_date},
        ).fetchone()

    if not result:
        return None

    current_data = deserialize_java_object(result[0])
    if current_data is None:
        return None

    forecast_obj = getattr(current_data, "forecast", None)
    if forecast_obj is None:
        return None

    data = extract_forecast_data(forecast_obj)
    if not data:
        return None

    df = pd.DataFrame(data)
    try:
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M")
    except Exception:
        return None

    # Задаване и сортиране на времевия индекс
    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)

    # Ресемплиране до 15-минутен интервал с линейна интерполация
    df_15min = df.resample("15min").interpolate(method="linear").reset_index()
    df_15min["time"] = df_15min["time"].dt.strftime("%Y-%m-%d %H:%M")
    return df_15min
