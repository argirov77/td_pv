import logging
import math
import os
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, validator

import forecast_db
from weather_api import get_forecast_by_coords

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_TOPICS_PER_REQUEST = int(os.getenv("MAX_TOPICS_PER_REQUEST", "100"))


class PredictRequest(BaseModel):
    prediction_date: str = Field(..., description="Дата във формат YYYY-MM-DD")
    topics: list[str] = Field(..., description="Списък от topic идентификатори")

    @validator("topics")
    def validate_topics(cls, value):
        if not value:
            raise ValueError("Полето topics не може да е празно.")
        if len(value) > MAX_TOPICS_PER_REQUEST:
            raise ValueError(
                f"Позволени са най-много {MAX_TOPICS_PER_REQUEST} теми на заявка."
            )
        return value

    class Config:
        schema_extra = {
            "example": {
                "prediction_date": "2025-03-20",
                "topics": [
                    "P0086H01/I002/Ptot",
                    "P0063H01/E001/Ptot",
                ],
            }
        }


def sanitize(val):
    """Преобразува невалидни числа (NaN, inf) в None."""
    if isinstance(val, float) and not math.isfinite(val):
        return None
    return val


@app.post("/predict")
def predict(request: PredictRequest):
    try:
        forecast_date = datetime.strptime(request.prediction_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Невалиден формат на дата. Очаква се YYYY-MM-DD.")

    start_ts = forecast_date.strftime("%Y-%m-%d 00:00")
    end_ts = (forecast_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00")

    try:
        points = forecast_db.select_points(request.topics, start_ts, end_ts)
    except Exception as exc:
        logger.exception("Грешка при извличане на точки от forecast_db")
        raise HTTPException(500, "Неуспешно извличане на данни за прогноза.") from exc

    response: dict[str, list[dict]] = {topic: [] for topic in request.topics}

    for point in points or []:
        topic = point.get("topic")
        if topic not in response:
            continue

        x_value = point.get("x") or point.get("ts") or point.get("time")
        y_value = sanitize(point.get("y") if "y" in point else point.get("value"))
        if x_value is None:
            continue

        response[topic].append({"x": str(x_value), "y": y_value})

    return response


@app.get("/weather/forecast")
def weather_forecast(
    lat: float = Query(..., description="Ширина"),
    lon: float = Query(..., description="Дължина"),
    date_str: str = Query(..., description="Дата във формат YYYY-MM-DD"),
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
