import requests
from datetime import date

from config import load_settings

settings = load_settings()
WEATHER_API_URL = "http://api.weatherapi.com/v1/forecast.json"


def get_forecast_by_coords(lat: float, lon: float, forecast_date: date):
    params = {
        "key": settings.weather_api_key,
        "q": f"{lat},{lon}",
        "dt": forecast_date.strftime("%Y-%m-%d"),
    }
    try:
        resp = requests.get(WEATHER_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        hours = data["forecast"]["forecastday"][0]["hour"]
        return [
            {
                "time": item["time"],
                "temp_c": item.get("temp_c"),
                "cloud": item.get("cloud"),
            }
            for item in hours
        ]
    except Exception:
        return []
