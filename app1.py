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
from weather_service import get_weather_for_date

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

THRESHOLD_RADIATION = 40

class PredictRequest(BaseModel):
    tag: str
    prediction_date: str = Field(..., description="Дата във формат YYYY-MM-DD")
    
    class Config:
        json_schema_extra = {
            "example": {
                "tag": "P0086H01/I002/Ptot",
                "prediction_date": "2025-03-20"
            }
        }

def sanitize_float_values(prediction: dict) -> dict:
    for key, value in prediction.items():
        if isinstance(value, float) and not math.isfinite(value):
            prediction[key] = None
    return prediction

@app.post("/predict")
def predict(request: PredictRequest):
    """
    Изчислява почасовото производство на системата.
    Връща:
      - time: "YYYY-MM-DD HH:MM"
      - temperature_c: температура (°C)
      - clear_sky_radiation_w_m2: стойност на clearsky радиация
      - model_radiation_w_m2: коригирана стойност на радиацията (ако моделът е наличен)
      - system_power: краен резултат - мощност на системата (W)
    Ако clearsky радиацията е под прага, системата не произвежда енергия.
    """
    try:
        try:
            forecast_date = datetime.strptime(request.prediction_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Невалиден формат на дата: {request.prediction_date}")
            raise HTTPException(status_code=400, detail="Невалиден формат на дата. Очаква се YYYY-MM-DD")
        
        spec = get_tag_specification(request.tag)
        if not spec:
            logger.error(f"Не е намерена спецификация за таг: {request.tag}")
            raise HTTPException(status_code=400, detail="Не е намерена спецификация за подадения таг")
        
        # Използваме полето sm_user_object_id за метеоданни
        sm_user_object_id = spec.get("sm_user_object_id")
        if not sm_user_object_id:
            logger.error("Липсва sm_user_object_id в спецификацията")
            raise HTTPException(status_code=400, detail="Липсва sm_user_object_id в спецификацията")
        
        latitude = spec.get("latitude")
        longitude = spec.get("longitude")
        if latitude is None or longitude is None:
            logger.error("Липсват координати в спецификацията")
            raise HTTPException(status_code=400, detail="Липсват координати в спецификацията")
        
        tilt = spec.get("tilt", 0.0)
        azimuth = spec.get("azimuth", 180.0)
        module_length = spec.get("module_length")
        module_width = spec.get("module_width")
        module_eff_percent = spec.get("module_efficiency", 17.7)
        total_panels = spec.get("total_panels")
        commissioning_date = spec.get("commissioning_date")
        degradation_rate = spec.get("degradation_rate", 0.0)
        
        if not module_length or not module_width:
            logger.error("Липсват размери на модула в спецификацията.")
            raise HTTPException(status_code=400, detail="Липсват размери на модула в спецификацията.")
        
        panel_area = (module_length / 1000) * (module_width / 1000)
        module_eff = module_eff_percent / 100.0

        weather_result = get_weather_for_date(
            user_object_id=int(sm_user_object_id),
            latitude=float(latitude),
            longitude=float(longitude),
            prediction_date=forecast_date,
        )
        weather_data = weather_result["records"]
        if weather_result["status"] == "no_data":
            return {
                "source": weather_result["source"],
                "status": "no_weather_data",
                "predictions": [],
            }
        
        model_file = f"{request.tag}_model.pkl"
        model = load_model(model_file)
        
        predictions = []
        for hour_data in weather_data:
            try:
                time_str = hour_data.get("time")
                if time_str is None:
                    hour_local = int(hour_data.get("hour_local", 0))
                    dt = datetime.combine(forecast_date, datetime.min.time()).replace(hour=hour_local)
                else:
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
                
                irradiance = calculate_panel_irradiance(
                    latitude=latitude,
                    longitude=longitude,
                    dt=dt,
                    panel_tilt=tilt,
                    panel_azimuth=azimuth,
                    tz="Europe/Nicosia"
                )
                
                if irradiance < THRESHOLD_RADIATION:
                    effective_radiation = 0.0
                else:
                    if model is not None:
                        input_df = pd.DataFrame({
                            "radiation_w_m2_y": [irradiance],
                            "cloud": [float(hour_data.get("cloud", 0))]
                        })
                        effective_radiation = model.predict(input_df)[0]
                    else:
                        effective_radiation = irradiance
                
                base_panel_power = effective_radiation * panel_area * module_eff
                temp_c = float(hour_data.get("temp_c", 25))
                cloud_pct = float(hour_data.get("cloud", 0))
                cloud_frac = cloud_pct / 100.0
                
                system_power = calculate_system_production(
                    panel_power=base_panel_power,
                    temp_c=temp_c,
                    cloud_cover=cloud_frac,
                    num_panels=total_panels,
                    forecast_date=dt,
                    commissioning_date=datetime.strptime(str(commissioning_date), "%Y-%m-%d"),
                    degradation_rate=degradation_rate
                )
                
                pred = {
                    "time": dt.strftime("%Y-%m-%d %H:%M"),
                    "system_power": system_power,
                    "source": weather_result["source"],
                }
                predictions.append(sanitize_float_values(pred))
            except Exception as e:
                logger.error(f"Грешка при обработка на часови данни {hour_data}: {e}")
                fallback_time = time_str or f"{forecast_date} {hour_data.get('hour_local', 0):02d}:00"
                predictions.append({
                    "time": fallback_time,
                    "system_power": 0
                })
        return {"source": weather_result["source"], "status": "ok", "predictions": predictions}
    except Exception as e:
        logger.exception("Ненадеждно хваната грешка в endpoint /predict")
        raise HTTPException(status_code=500, detail="Вътрешна сървърна грешка")



class WeatherInfoResponse(BaseModel):
    source: str
    status: str
    points: list[dict] = Field(default_factory=list)

class WeatherInfoRequest(BaseModel):
    tag: str
    prediction_date: str = Field(..., description="Дата във формат YYYY-MM-DD")
    
    class Config:
        json_schema_extra = {
            "example": {
                "tag": "P0086H01/I002/Ptot",
                "prediction_date": "2025-03-20"
            }
        }

@app.post("/weather_info", response_model=WeatherInfoResponse)
def weather_info(request: WeatherInfoRequest):
    """
    Извлича метеоданни (температура и облачност) за посочения обект.
    Използва поле sm_user_object_id за извличане от weather_data.
    Връща списък записи с полета:
      - time: време на измерване
      - temp_c: температура (°C)
      - cloud: облачност (в %)
    """
    try:
        try:
            forecast_date = datetime.strptime(request.prediction_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Невалиден формат на дата: {request.prediction_date}")
            raise HTTPException(status_code=400, detail="Невалиден формат на дата. Очаква се YYYY-MM-DD")
        
        spec = get_tag_specification(request.tag)
        if not spec:
            logger.error(f"Не е намерена спецификация за таг: {request.tag}")
            raise HTTPException(status_code=400, detail="Не е намерена спецификация за подадения таг")
        
        sm_user_object_id = spec.get("sm_user_object_id")
        if not sm_user_object_id:
            logger.error("Липсва sm_user_object_id в спецификацията")
            raise HTTPException(status_code=400, detail="Липсва sm_user_object_id в спецификацията")

        latitude = spec.get("latitude")
        longitude = spec.get("longitude")
        if latitude is None or longitude is None:
            logger.error("Липсват координати в спецификацията")
            raise HTTPException(status_code=400, detail="Липсват координати в спецификацията")

        weather_result = get_weather_for_date(
            user_object_id=int(sm_user_object_id),
            latitude=float(latitude),
            longitude=float(longitude),
            prediction_date=forecast_date,
        )
        weather_data = weather_result["records"]
        if weather_result["status"] == "no_data":
            return {"source": weather_result["source"], "status": "no_data", "points": []}
        
        # Ако данните са получени като списък записи, филтрираме всеки запис, оставяйки нужните полета
        sanitized = []
        for rec in weather_data:
            new_rec = {}
            for k, v in rec.items():
                # Ако v е число и е NaN, заменяме с None.
                if isinstance(v, float) and (v != v):
                    new_rec[k] = None
                else:
                    new_rec[k] = v
            # Оставяме само нужните ключове: time, temp_c и cloud.
            sanitized.append({
                "time": new_rec.get("time"),
                "temp_c": new_rec.get("temp_c"),
                "cloud": new_rec.get("cloud")
            })
        return {"source": weather_result["source"], "status": "ok", "points": sanitized}
    except Exception as e:
        logger.exception("Ненадеждно хваната грешка в endpoint /weather_info")
        raise HTTPException(status_code=500, detail="Вътрешна сървърна грешка")
