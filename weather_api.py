import os
import requests
from datetime import date, timedelta
import pandas as pd

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "570662416a664e7bbb685714250402")
WEATHER_API_URL = "http://api.weatherapi.com/v1/forecast.json"

def interpolate_15min(values, date_str):
    """
    Преобразует массив из 24 значений (по часу) в 96 значений (по 15 минут).
    """
    times = pd.date_range(f"{date_str} 00:00", f"{date_str} 23:00", freq="1H")
    s = pd.Series(values, index=times)
    new_times = pd.date_range(f"{date_str} 00:00", f"{date_str} 23:45", freq="15T")
    s_interp = s.reindex(new_times).interpolate('linear')
    return [t.strftime("%Y-%m-%d %H:%M") for t in new_times], s_interp.tolist()

def get_forecast_by_coords(lat: float, lon: float, forecast_date: date):
    """
    Получает прогноз (температура, облачность, время) по координатам и дате с шагом 15 минут.
    Температура — float до десятых, облачность — int.
    """
    query = f"{lat},{lon}"
    params = {
        "key": WEATHER_API_KEY,
        "q": query,
        "dt": forecast_date.strftime("%Y-%m-%d"),
    }

    try:
        resp = requests.get(WEATHER_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        hours = data["forecast"]["forecastday"][0]["hour"]
        temp_c = [h["temp_c"] for h in hours]
        cloud = [h["cloud"] for h in hours]
        time = [h["time"] for h in hours]

        date_str = forecast_date.strftime("%Y-%m-%d")
        time_15min, temp_c_15min = interpolate_15min(temp_c, date_str)
        _, cloud_15min = interpolate_15min(cloud, date_str)

        # Округляем
        temp_c_15min = [round(val, 1) if val is not None else None for val in temp_c_15min]
        cloud_15min = [int(round(val)) if val is not None else None for val in cloud_15min]

        result = {
            "date": date_str,
            "location": data["location"]["name"],
            "lat": lat,
            "lon": lon,
            "time": time_15min,
            "temp_c": temp_c_15min,
            "cloud": cloud_15min
        }
        return result

    except Exception as e:
        print(f"WeatherAPI error: {e}")
        return None

# Для теста:
if __name__ == "__main__":
    tomorrow = date.today() + timedelta(days=1)
    res = get_forecast_by_coords(42.6977, 23.3219, tomorrow)
    print(res)
