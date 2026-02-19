from datetime import datetime, timedelta

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field

from config import load_settings
from database import (
    DatabaseReadError,
    get_all_topic_specifications_or_raise,
    get_all_topics_or_raise,
)
from forecast_db import run_migrations, select_available_forecasts, select_points
from jobs.history_service import history_job_service

settings = load_settings()
app = FastAPI()


class PredictRequest(BaseModel):
    prediction_date: str = Field(..., description="Дата във формат YYYY-MM-DD")
    topics: list[str] = Field(default_factory=list)


class TopicItem(BaseModel):
    tag: str


class TopicListResponse(BaseModel):
    topics: list[TopicItem] = Field(default_factory=list)


class TopicSpecItem(BaseModel):
    tag: str
    sm_user_object_id: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    tilt: float | None = None
    azimuth: float | None = None
    module_length: float | None = None
    module_width: float | None = None
    module_efficiency: float | None = None
    total_panels: int | None = None


class TopicSpecListResponse(BaseModel):
    specs: list[TopicSpecItem] = Field(default_factory=list)


class AvailableForecastsResponse(BaseModel):
    count: int
    topics: list[str] = Field(default_factory=list)
    dates: list[str] = Field(default_factory=list)


class GenerateHistoryRequest(BaseModel):
    days: int | None = Field(default=None, ge=1, description="Брой дни назад за генериране")


class JobResponse(BaseModel):
    id: str
    state: str
    days: int
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None


class GenerateHistoryResponse(BaseModel):
    started: bool
    job: JobResponse


@app.on_event("startup")
def startup() -> None:
    run_migrations()


@app.get("/forecasts/available", response_model=AvailableForecastsResponse)
def get_available_forecasts(
    topic: str | None = Query(default=None),
    date_from: str | None = Query(default=None, description="Дата от (вкл.) във формат YYYY-MM-DD"),
    date_to: str | None = Query(default=None, description="Дата до (изкл.) във формат YYYY-MM-DD"),
) -> AvailableForecastsResponse:
    parsed_date_from: datetime | None = None
    parsed_date_to: datetime | None = None

    if date_from:
        try:
            parsed_date_from = datetime.strptime(date_from, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Невалиден date_from. Очаква се YYYY-MM-DD.")

    if date_to:
        try:
            parsed_date_to = datetime.strptime(date_to, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Невалиден date_to. Очаква се YYYY-MM-DD.")

    if parsed_date_from and parsed_date_to and parsed_date_from >= parsed_date_to:
        raise HTTPException(status_code=400, detail="date_from трябва да е по-малка от date_to.")

    payload = select_available_forecasts(topic=topic, date_from=parsed_date_from, date_to=parsed_date_to)
    return AvailableForecastsResponse(**payload)


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


@app.post("/jobs/generate-history", response_model=GenerateHistoryResponse, status_code=status.HTTP_202_ACCEPTED)
def generate_history_job(
    payload: GenerateHistoryRequest,
    background_tasks: BackgroundTasks,
) -> GenerateHistoryResponse:
    days = payload.days if payload.days is not None else settings.forecast_history_days
    if days < 1:
        raise HTTPException(status_code=400, detail="days трябва да е положително число.")

    creation = history_job_service.create_job(days=days)
    job = creation["job"]

    if creation["started"]:
        background_tasks.add_task(history_job_service.run_job, job["id"])

    return GenerateHistoryResponse(started=creation["started"], job=JobResponse(**job))


@app.get("/jobs/{job_id}", response_model=JobResponse)
def get_job_status(job_id: str) -> JobResponse:
    job = history_job_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobResponse(**job)


@app.get("/topics", response_model=TopicListResponse)
def get_topics() -> TopicListResponse:
    try:
        topics = get_all_topics_or_raise()
    except DatabaseReadError:
        raise HTTPException(status_code=503, detail="Базата данни е недостъпна. Опитайте по-късно.")

    return TopicListResponse(topics=[TopicItem(tag=tag) for tag in topics])


@app.get("/topics/specs", response_model=TopicSpecListResponse)
def get_topic_specs() -> TopicSpecListResponse:
    try:
        specs = get_all_topic_specifications_or_raise()
    except DatabaseReadError:
        raise HTTPException(status_code=503, detail="Базата данни е недостъпна. Опитайте по-късно.")

    return TopicSpecListResponse(specs=[TopicSpecItem(**spec) for spec in specs])
