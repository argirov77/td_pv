from datetime import datetime, timedelta
from typing import Literal

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from config import load_settings
from database import (
    DatabaseReadError,
    get_all_topic_specifications_or_raise,
    get_all_topics_or_raise,
    get_tag_specification,
)
from forecast_db import run_migrations, select_available_forecasts, select_points
from jobs.history_service import history_job_service
from radiation import calculate_panel_irradiance
from weather_service import get_weather_for_date

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




class WeatherInfoRequest(BaseModel):
    tag: str
    prediction_date: str = Field(..., description="Дата във формат YYYY-MM-DD")


class WeatherPoint(BaseModel):
    time: str | None = None
    temp_c: float | None = None
    cloud: int | None = None


class WeatherInfoResponse(BaseModel):
    source: Literal["archive_db", "weather_api", "none"]
    status: Literal["ok", "no_data"]
    points: list[WeatherPoint] = Field(default_factory=list)

class ClearSkyRadiationRequest(BaseModel):
    tag: str | None = Field(default=None, description="Таг от tag_specification")
    lat: float | None = Field(default=None, description="Географска ширина")
    lon: float | None = Field(default=None, description="Географска дължина")
    date: str = Field(..., description="Дата във формат YYYY-MM-DD")
    tilt: float | None = Field(default=None, description="Наклон на панела")
    azimuth: float | None = Field(default=None, description="Азимут на панела")
    step_minutes: Literal[15, 60] = Field(default=60, description="Стъпка на времевия ред в минути")


class ClearSkyPoint(BaseModel):
    time: str
    clear_sky_radiation_w_m2: float


class ClearSkyRadiationResponse(BaseModel):
    source: Literal["tag", "coordinates"]
    tag: str | None = None
    latitude: float
    longitude: float
    tilt: float
    azimuth: float
    date: str
    step_minutes: int
    points: list[ClearSkyPoint] = Field(default_factory=list)


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




@app.post("/weather_info", response_model=WeatherInfoResponse)
def weather_info(request: WeatherInfoRequest) -> WeatherInfoResponse:
    try:
        forecast_date = datetime.strptime(request.prediction_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Невалиден формат на дата. Очаква се YYYY-MM-DD")

    spec = get_tag_specification(request.tag)
    if not spec:
        raise HTTPException(status_code=400, detail="Не е намерена спецификация за подадения таг")

    sm_user_object_id = spec.get("sm_user_object_id")
    if sm_user_object_id is None:
        raise HTTPException(status_code=400, detail="Липсва sm_user_object_id в спецификацията")

    latitude = spec.get("latitude")
    longitude = spec.get("longitude")
    if latitude is None or longitude is None:
        raise HTTPException(status_code=400, detail="Липсват координати в спецификацията")

    weather_result = get_weather_for_date(
        user_object_id=int(sm_user_object_id),
        latitude=float(latitude),
        longitude=float(longitude),
        prediction_date=forecast_date,
    )

    sanitized_points: list[WeatherPoint] = []
    for rec in weather_result["records"]:
        cloud_value = rec.get("cloud")
        cloud_int: int | None
        if cloud_value is None:
            cloud_int = None
        else:
            try:
                cloud_int = int(round(float(cloud_value)))
            except (TypeError, ValueError):
                cloud_int = None

        temp_value = rec.get("temp_c")
        temp_float: float | None
        if temp_value is None:
            temp_float = None
        else:
            try:
                temp_float = float(temp_value)
            except (TypeError, ValueError):
                temp_float = None

        sanitized_points.append(
            WeatherPoint(
                time=rec.get("time"),
                temp_c=temp_float,
                cloud=cloud_int,
            )
        )

    return WeatherInfoResponse(
        source=weather_result["source"],
        status=weather_result["status"],
        points=sanitized_points,
    )

@app.post("/radiation/clear-sky", response_model=ClearSkyRadiationResponse)
def calculate_clear_sky_radiation(request: ClearSkyRadiationRequest) -> ClearSkyRadiationResponse:
    try:
        day_start = datetime.strptime(request.date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Невалиден формат на дата. Очаква се YYYY-MM-DD.")

    if request.tag is None and (request.lat is None or request.lon is None):
        raise HTTPException(status_code=400, detail="Подайте или tag, или едновременно lat/lon.")

    source: Literal["tag", "coordinates"]
    tag: str | None = None
    latitude: float
    longitude: float
    tilt: float
    azimuth: float

    if request.tag:
        spec = get_tag_specification(request.tag)
        if not spec:
            raise HTTPException(status_code=404, detail="Не е намерена спецификация за подадения tag.")

        if spec.get("latitude") is None or spec.get("longitude") is None:
            raise HTTPException(status_code=400, detail="Липсват координати в спецификацията за tag.")

        source = "tag"
        tag = request.tag
        latitude = float(spec["latitude"])
        longitude = float(spec["longitude"])
        tilt = float(spec.get("tilt") or 0.0)
        azimuth = float(spec.get("azimuth") or 180.0)
    else:
        source = "coordinates"
        latitude = float(request.lat)
        longitude = float(request.lon)
        tilt = float(request.tilt or 0.0)
        azimuth = float(request.azimuth or 180.0)

    points: list[ClearSkyPoint] = []
    intervals = (24 * 60) // request.step_minutes
    for i in range(intervals):
        dt = day_start + timedelta(minutes=i * request.step_minutes)
        irradiance = calculate_panel_irradiance(
            latitude=latitude,
            longitude=longitude,
            dt=dt,
            panel_tilt=tilt,
            panel_azimuth=azimuth,
            tz="Europe/Nicosia",
        )
        points.append(
            ClearSkyPoint(
                time=dt.strftime("%Y-%m-%d %H:%M"),
                clear_sky_radiation_w_m2=irradiance,
            )
        )

    return ClearSkyRadiationResponse(
        source=source,
        tag=tag,
        latitude=latitude,
        longitude=longitude,
        tilt=tilt,
        azimuth=azimuth,
        date=request.date,
        step_minutes=request.step_minutes,
        points=points,
    )


@app.get("/test-ui", response_class=HTMLResponse)
def test_ui() -> HTMLResponse:
    return HTMLResponse(
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>PV Test UI</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    .panel { max-width: 1100px; margin-bottom: 24px; }
    form { display: flex; gap: 12px; margin-bottom: 12px; }
    input, button { padding: 8px; }
    canvas { width: 100%; height: 300px; }
  </style>
</head>
<body>
  <h2>PV Test UI</h2>
  <form id="controls">
    <input id="tag" placeholder="tag" value="P0086H01/I002/Ptot" />
    <input id="date" type="date" />
    <button type="submit">Load</button>
  </form>

  <div class="panel">
    <h3>Actual/Model values (placeholder)</h3>
    <canvas id="mainChart"></canvas>
  </div>

  <div class="panel">
    <h3>Clear-sky radiation</h3>
    <canvas id="clearSkyChart"></canvas>
  </div>

  <script>
    const dateInput = document.getElementById('date');
    dateInput.value = new Date().toISOString().slice(0, 10);

    const mainChart = new Chart(document.getElementById('mainChart'), {
      type: 'line',
      data: { labels: [], datasets: [] },
      options: { responsive: true, maintainAspectRatio: false }
    });

    const clearSkyChart = new Chart(document.getElementById('clearSkyChart'), {
      type: 'line',
      data: { labels: [], datasets: [{ label: 'Clear-sky radiation (W/m²)', data: [], borderColor: '#1f77b4', tension: 0.2 }] },
      options: { responsive: true, maintainAspectRatio: false }
    });

    document.getElementById('controls').addEventListener('submit', async (e) => {
      e.preventDefault();
      const payload = {
        tag: document.getElementById('tag').value,
        date: dateInput.value,
        step_minutes: 15,
      };

      const res = await fetch('/radiation/clear-sky', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const data = await res.json();
      const labels = data.points.map((p) => p.time.slice(11));
      const values = data.points.map((p) => p.clear_sky_radiation_w_m2);

      clearSkyChart.data.labels = labels;
      clearSkyChart.data.datasets[0].data = values;
      clearSkyChart.update();
    });
  </script>
</body>
</html>
        """
    )
