from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from datetime import datetime, date
import logging
import pandas as pd
import math

from database import get_tag_specification
from radiation import calculate_panel_irradiance
from production import calculate_system_production
from model_loader import load_model
from weather_db import extract_weather_from_db
from weather_api import get_forecast_by_coords  

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

THRESHOLD_RADIATION = 40  # W/m²

class PredictRequest(BaseModel):
    prediction_date: str = Field(..., description="Дата във формат YYYY-MM-DD")
    tag: str = Field(..., alias="topic", description="Идентификатор на таг (или топик)")

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "prediction_date": "2025-03-20",
                "topic": "P0086H01/I002/Ptot"
            }
        }

def sanitize(val):
    """Преобразува невалидни числа (NaN, inf) в None."""
    if isinstance(val, float) and not math.isfinite(val):
        return None
    return val

@app.post("/predict")
def predict(request: PredictRequest):
    # 1) Парсване на дата
    try:
        forecast_date = datetime.strptime(request.prediction_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Невалиден формат на дата. Очаква се YYYY-MM-DD.")

    today = date.today()

    # 2) Вземане на техническа спецификация по таг
    tag = request.tag
    spec = get_tag_specification(tag)
    if not spec:
        raise HTTPException(400, f"Няма спецификация за таг '{tag}'.")

    # 3) Извличане на координати и параметри на панела
    uid = spec.get("sm_user_object_id")
    if not uid:
        raise HTTPException(400, "Липсва 'sm_user_object_id' в спецификацията.")
    lat = spec.get("latitude")
    lon = spec.get("longitude")
    if lat is None or lon is None:
        raise HTTPException(400, "Липсват координати (latitude/longitude) в спецификацията.")

    tilt = spec.get("tilt", 0.0)
    azimuth = spec.get("azimuth", 180.0)
    mlen = spec.get("module_length")
    mwid = spec.get("module_width")
    meff_pct = spec.get("module_efficiency", 17.7)
    panels = spec.get("total_panels")
    comm = spec.get("commissioning_date")
    degr = spec.get("degradation_rate", 0.0)

    if not mlen or not mwid:
        raise HTTPException(400, "Липсват размери на панела в спецификацията.")

    panel_area = (mlen/1000) * (mwid/1000)
    mod_eff = meff_pct/100.0

    result = []

    # 4) Избор на източник на метео-данни (исторически или прогноза)
    if forecast_date >= today:
        # --- Ако датата е днес или в бъдещето — взимаме прогнозата с интервал 15 мин ---
        weather = get_forecast_by_coords(lat, lon, forecast_date)
        if not weather:
            raise HTTPException(502, "Грешка при вземане на прогноза от WeatherAPI")

        # weather е dict с time, temp_c, cloud (всички са с дължина 96)
        for t, temp_c, cloud in zip(weather["time"], weather["temp_c"], weather["cloud"]):
            dt = datetime.strptime(t, "%Y-%m-%d %H:%M")
            irr = calculate_panel_irradiance(
                latitude=lat,
                longitude=lon,
                dt=dt,
                panel_tilt=tilt,
                panel_azimuth=azimuth,
                tz="Europe/Nicosia"
            )

            # Минимален праг по радиация
            if irr < THRESHOLD_RADIATION:
                eff = 0.0
            else:
                eff = irr  # Може да добавиш ML модел при нужда

            base = eff * panel_area * mod_eff
            temp_c_val = float(temp_c) if temp_c is not None else 25.0
            cloud_frac = float(cloud) / 100.0 if cloud is not None else 0.0

            power = calculate_system_production(
                panel_power=base,
                temp_c=temp_c_val,
                cloud_cover=cloud_frac,
                num_panels=panels,
                forecast_date=dt,
                commissioning_date=datetime.strptime(str(comm), "%Y-%m-%d"),
                degradation_rate=degr
            )

            result.append({
                "x": dt.strftime("%Y-%m-%d %H:%M"),
                "y": sanitize(power)
            })
        return result

    else:
        # --- Ако датата е в миналото — взимаме исторически данни от базата ---
        weather = extract_weather_from_db(uid, request.prediction_date)
        if not weather:
            raise HTTPException(404, "Няма метео-данни за този обект/дата.")

        model_name = tag.replace("/", "_") + "_model.pkl"
        model = load_model(model_name)

        for rec in weather:
            tstr = rec.get("time")
            if tstr:
                try:
                    dt = datetime.strptime(tstr, "%Y-%m-%d %H:%M")
                except:
                    continue
            else:
                hl = int(rec.get("hour_local", 0))
                dt = datetime.combine(forecast_date, datetime.min.time()).replace(hour=hl)

            irr = calculate_panel_irradiance(
                latitude=lat,
                longitude=lon,
                dt=dt,
                panel_tilt=tilt,
                panel_azimuth=azimuth,
                tz="Europe/Nicosia"
            )

            if irr < THRESHOLD_RADIATION:
                eff = 0.0
            else:
                if model:
                    df_in = pd.DataFrame({
                        "radiation_w_m2_y": [irr],
                        "cloud": [float(rec.get("cloud", 0))]
                    })
                    eff = float(model.predict(df_in)[0])
                else:
                    eff = irr

            base = eff * panel_area * mod_eff
            temp_c = float(rec.get("temp_c", 25))
            cloud_frac = float(rec.get("cloud", 0))/100.0

            power = calculate_system_production(
                panel_power=base,
                temp_c=temp_c,
                cloud_cover=cloud_frac,
                num_panels=panels,
                forecast_date=dt,
                commissioning_date=datetime.strptime(str(comm), "%Y-%m-%d"),
                degradation_rate=degr
            )

            result.append({
                "x": dt.strftime("%Y-%m-%d %H:%M"),
                "y": sanitize(power)
            })

        return result

@app.get("/weather/forecast")
def weather_forecast(
    lat: float = Query(..., description="Ширина"),
    lon: float = Query(..., description="Дължина"),
    date_str: str = Query(..., description="Дата във формат YYYY-MM-DD")
):
    """
    Връща прогнозата за облачност и температура по координати и дата (96 точки, 15-минутен интервал)
    """
    try:
        forecast_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Невалиден формат на дата. Използвай YYYY-MM-DD.")

    data = get_forecast_by_coords(lat, lon, forecast_date)
    if not data:
        raise HTTPException(502, "Грешка при вземане на данни от WeatherAPI")

    return data
