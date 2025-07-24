from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
import logging
import pandas as pd
import math

from database import get_tag_specification
from radiation import calculate_panel_irradiance
from production import calculate_system_production
from model_loader import load_model
from weather_db import extract_weather_from_db

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

THRESHOLD_RADIATION = 40  # W/m²


class PredictRequest(BaseModel):
    prediction_date: str = Field(..., description="Date in YYYY-MM-DD format")
    tag: str = Field(..., alias="topic", description="Tag (or topic) identifier")

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "prediction_date": "2025-03-20",
                "topic": "P0086H01/I002/Ptot"
            }
        }


def sanitize(val):
    """Convert non-finite floats (NaN, inf) to None."""
    if isinstance(val, float) and not math.isfinite(val):
        return None
    return val


@app.post("/predict")
def predict(request: PredictRequest):
    # 1) parse date
    try:
        forecast_date = datetime.strptime(request.prediction_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Invalid date format. Expected YYYY-MM-DD.")

    # 2) fetch spec
    tag = request.tag
    spec = get_tag_specification(tag)
    if not spec:
        raise HTTPException(400, f"No specification found for tag '{tag}'.")

    # 3) pull out coordinates & module info
    uid = spec.get("sm_user_object_id")
    if not uid:
        raise HTTPException(400, "Specification missing 'sm_user_object_id'.")
    lat = spec.get("latitude"); lon = spec.get("longitude")
    if lat is None or lon is None:
        raise HTTPException(400, "Specification missing latitude/longitude.")

    tilt = spec.get("tilt", 0.0)
    azimuth = spec.get("azimuth", 180.0)
    mlen = spec.get("module_length"); mwid = spec.get("module_width")
    meff_pct = spec.get("module_efficiency", 17.7)
    panels = spec.get("total_panels")
    comm = spec.get("commissioning_date")
    degr = spec.get("degradation_rate", 0.0)

    if not mlen or not mwid:
        raise HTTPException(400, "Specification missing module dimensions.")

    panel_area = (mlen/1000) * (mwid/1000)
    mod_eff = meff_pct/100.0

    # 4) get weather records
    weather = extract_weather_from_db(uid, request.prediction_date)
    if not weather:
        raise HTTPException(404, "No weather data for this object/date.")

    # 5) load ML model
    model_name = tag.replace("/", "_") + "_model.pkl"
    model = load_model(model_name)

    # 6) iterate & compute
    result = []
    for rec in weather:
        # timestamp
        tstr = rec.get("time")
        if tstr:
            try:
                dt = datetime.strptime(tstr, "%Y-%m-%d %H:%M")
            except:
                continue
        else:
            hl = int(rec.get("hour_local", 0))
            dt = datetime.combine(forecast_date, datetime.min.time()).replace(hour=hl)

        # clearsky POA
        irr = calculate_panel_irradiance(
            latitude=lat,
            longitude=lon,
            dt=dt,
            panel_tilt=tilt,
            panel_azimuth=azimuth,
            tz="Europe/Nicosia"
        )

        # apply threshold + optional ML correction
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
