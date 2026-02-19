from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config import load_settings
from forecast_db import run_migrations, select_points
from routes.test_ui import router as test_ui_router

settings = load_settings()
app = FastAPI()
app.include_router(test_ui_router)


class PredictRequest(BaseModel):
    prediction_date: str = Field(..., description="Дата във формат YYYY-MM-DD")
    topics: list[str] = Field(default_factory=list)


@app.on_event("startup")
def startup() -> None:
    run_migrations()


@app.post("/predict")
def predict(request: PredictRequest):
    if len(request.topics) > settings.max_topics_per_request:
        raise HTTPException(status_code=400, detail=f"topics limit exceeded: {settings.max_topics_per_request}")

    try:
        day_start = datetime.strptime(request.prediction_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Невалиден формат на дата. Очаква се YYYY-MM-DD.")

    day_end = day_start + timedelta(days=1)
    return select_points(request.topics, day_start, day_end)
