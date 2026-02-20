from datetime import datetime, timedelta
import math
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
from jobs.generate_forecasts import _build_rows_for_topic
from jobs.history_service import history_job_service
from radiation import calculate_panel_irradiance
from weather_service import get_weather_for_date

settings = load_settings()
app = FastAPI()


class PredictRequest(BaseModel):
    prediction_date: str = Field(..., description="Дата във формат YYYY-MM-DD")
    topics: list[str] = Field(default_factory=list)


class PredictionPoint(BaseModel):
    x: str
    y: float
    source: str


class PredictResponse(BaseModel):
    mode: Literal["cache", "recompute"]
    points: dict[str, list[PredictionPoint]] = Field(default_factory=dict)


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
    diagnostics: dict[str, str] | None = None

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


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    if len(request.topics) > settings.max_topics_per_request:
        raise HTTPException(status_code=400, detail=f"topics limit exceeded: {settings.max_topics_per_request}")

    try:
        day_start = datetime.strptime(request.prediction_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Невалиден формат на дата. Очаква се YYYY-MM-DD.")

    day_end = day_start + timedelta(days=1)
    return PredictResponse(mode="cache", points=select_points(request.topics, day_start, day_end))


@app.post("/predict/runtime", response_model=PredictResponse)
def predict_runtime(request: PredictRequest) -> PredictResponse:
    if len(request.topics) > settings.max_topics_per_request:
        raise HTTPException(status_code=400, detail=f"topics limit exceeded: {settings.max_topics_per_request}")

    try:
        day_start = datetime.strptime(request.prediction_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Невалиден формат на дата. Очаква се YYYY-MM-DD.")

    day_end = day_start + timedelta(days=1)
    points: dict[str, list[PredictionPoint]] = {topic: [] for topic in request.topics}

    for topic in request.topics:
        spec = get_tag_specification(topic)
        if not spec:
            continue

        uid = spec.get("sm_user_object_id")
        lat = spec.get("latitude")
        lon = spec.get("longitude")
        if uid is None or lat is None or lon is None:
            continue

        weather_result = get_weather_for_date(
            user_object_id=int(uid),
            latitude=float(lat),
            longitude=float(lon),
            prediction_date=day_start.date(),
        )
        rows = _build_rows_for_topic(topic, weather_result["records"], weather_result["source"])
        points[topic] = [
            PredictionPoint(x=ts.strftime("%Y-%m-%d %H:%M"), y=power, source=source)
            for _, ts, power, source in rows
            if day_start <= ts < day_end
        ]

    return PredictResponse(mode="recompute", points=points)


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
                cloud_float = float(cloud_value)
                cloud_int = int(round(cloud_float)) if math.isfinite(cloud_float) else None
            except (TypeError, ValueError):
                cloud_int = None

        temp_value = rec.get("temp_c")
        temp_float: float | None
        if temp_value is None:
            temp_float = None
        else:
            try:
                parsed_temp = float(temp_value)
                temp_float = parsed_temp if math.isfinite(parsed_temp) else None
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
        diagnostics=weather_result.get("diagnostics"),
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
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>PV API Test Console</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --bg: #f6f8fb;
      --text: #0f172a;
      --panel: #ffffff;
      --border: #d1d5db;
      --success: #16a34a;
      --warning: #d97706;
      --error: #dc2626;
      --info: #2563eb;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Arial, sans-serif; color: var(--text); background: var(--bg); }
    .app { padding: 16px; display: flex; flex-direction: column; gap: 12px; }
    .header { display: flex; justify-content: space-between; align-items: center; background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; }
    .layout { display: grid; grid-template-columns: 320px 1fr 420px; gap: 12px; }
    .column { display: flex; flex-direction: column; gap: 12px; min-width: 0; }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
    h1, h2, h3, h4 { margin: 0 0 10px 0; }
    h1 { font-size: 20px; }
    h3 { font-size: 16px; }
    .status-pill { padding: 4px 10px; border-radius: 999px; color: #fff; font-size: 12px; font-weight: 700; }
    .status-ok { background: var(--success); }
    .status-warning { background: var(--warning); }
    .status-error { background: var(--error); }
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .field { display: flex; flex-direction: column; gap: 4px; margin-bottom: 8px; }
    label { font-size: 12px; font-weight: 700; }
    input, select, button, textarea { width: 100%; padding: 8px; border: 1px solid var(--border); border-radius: 6px; font: inherit; }
    button { cursor: pointer; background: #fff; }
    button:hover:not(:disabled) { border-color: var(--info); }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    .actions { display: grid; gap: 6px; }
    .actions-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
    .inline { display: flex; align-items: center; gap: 8px; }
    .meta { font-size: 12px; color: #475569; }
    .endpoint-status { font-size: 12px; margin-top: 4px; }
    .ok { color: var(--success); }
    .warn { color: var(--warning); }
    .err { color: var(--error); }
    .chart-wrap { height: 260px; }
    canvas { width: 100%; height: 100%; }
    .table-scroll { max-height: 220px; overflow: auto; border: 1px solid var(--border); border-radius: 6px; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { border-bottom: 1px solid #e5e7eb; padding: 6px; text-align: left; }
    th { position: sticky; top: 0; background: #f8fafc; }
    pre { background: #0b1020; color: #dbeafe; padding: 10px; border-radius: 6px; max-height: 360px; overflow: auto; font-size: 12px; }
    .log-list { max-height: 240px; overflow: auto; font-size: 12px; }
    .log-item { padding: 6px 0; border-bottom: 1px solid #e5e7eb; }
    .summary-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
    .summary-card { border: 1px solid var(--border); border-radius: 6px; padding: 8px; background: #f8fafc; }
    @media (max-width: 1250px) {
      .layout { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="app">
    <div class="header">
      <div>
        <h1>PV API Test Console</h1>
        <div class="meta">Smoke-путь API: topics → weather → clear-sky → predict → history job → forecasts available</div>
      </div>
      <div class="inline">
        <span id="backendStatus" class="status-pill status-warning">Backend: checking…</span>
        <button id="resetSessionBtn" type="button">Reset Session</button>
      </div>
    </div>

    <div class="layout">
      <div class="column">
        <section class="card">
          <h3>Контекст запроса</h3>
          <div class="field"><label for="baseUrl">Base URL</label><input id="baseUrl" /></div>
          <div class="field"><label for="dateInput">Date</label><input id="dateInput" type="date" /></div>
          <div class="field"><label for="topicSelect">Topic</label><select id="topicSelect"><option value="">-- выберите topic --</option></select></div>
          <div class="field"><label for="stepMinutes">Step minutes</label><select id="stepMinutes"><option value="15">15</option><option value="60">60</option></select></div>
          <div class="actions-2">
            <button id="loadTopicsBtn" type="button">Load Topics</button>
            <button id="loadSpecsBtn" type="button">Load Topic Specs</button>
          </div>
        </section>

        <section class="card">
          <h3>Погода и радиация</h3>
          <div class="actions-2">
            <button id="weatherBtn" type="button">Get Weather</button>
            <button id="clearSkyBtn" type="button">Get Clear-Sky</button>
          </div>
          <div id="weatherStatus" class="endpoint-status"></div>
          <div id="clearSkyStatus" class="endpoint-status"></div>
        </section>

        <section class="card">
          <h3>Прогноз мощности</h3>
          <div class="actions">
            <button id="predictCacheBtn" type="button">Predict (Cache)</button>
            <button id="predictRuntimeBtn" type="button">Predict (Runtime)</button>
            <button id="compareBtn" type="button">Compare Cache vs Runtime</button>
          </div>
          <div id="predictStatus" class="endpoint-status"></div>
        </section>

        <section class="card">
          <h3>Исторический job</h3>
          <div class="field"><label for="daysInput">days</label><input id="daysInput" type="number" min="1" value="3" /></div>
          <div class="field"><label for="jobIdInput">job_id</label><input id="jobIdInput" placeholder="появится после старта" /></div>
          <div class="actions-2">
            <button id="startJobBtn" type="button">Start Generate History</button>
            <button id="checkJobBtn" type="button">Check Job Status</button>
          </div>
          <div class="inline" style="margin-top: 8px;"><input id="autoPollToggle" type="checkbox" style="width:auto" /><label for="autoPollToggle" style="margin:0;">Auto-poll (3s)</label></div>
          <div id="jobStatus" class="endpoint-status"></div>
        </section>

        <section class="card">
          <h3>Наличие прогнозов</h3>
          <div class="field"><label for="forecastTopic">topic (optional)</label><input id="forecastTopic" /></div>
          <div class="grid-2">
            <div class="field"><label for="dateFrom">date_from</label><input id="dateFrom" type="date" /></div>
            <div class="field"><label for="dateTo">date_to</label><input id="dateTo" type="date" /></div>
          </div>
          <button id="availableBtn" type="button">Get Available Forecasts</button>
        </section>

        <section class="card">
          <button id="smokeBtn" type="button">Run Smoke Flow</button>
        </section>
      </div>

      <div class="column">
        <section class="card">
          <h3>Weather</h3>
          <div class="chart-wrap"><canvas id="weatherChart"></canvas></div>
          <div class="table-scroll"><table id="weatherTable"></table></div>
        </section>
        <section class="card">
          <h3>Clear-Sky Radiation</h3>
          <div class="chart-wrap"><canvas id="clearSkyChart"></canvas></div>
        </section>
        <section class="card">
          <h3>Power Predict (Cache/Runtime)</h3>
          <div class="chart-wrap"><canvas id="powerChart"></canvas></div>
          <div class="table-scroll"><table id="powerTable"></table></div>
        </section>
      </div>

      <div class="column">
        <section class="card">
          <h3>Диагностика</h3>
          <div class="meta">HTTP status: <span id="httpStatus">-</span> · latency: <span id="httpLatency">-</span></div>
          <div style="margin:8px 0;"><button id="copyJsonBtn" type="button">Copy JSON</button></div>
          <pre id="jsonPreview">{}</pre>
        </section>
        <section class="card">
          <h3>Последние действия</h3>
          <div id="actionLog" class="log-list"></div>
        </section>
        <section class="card">
          <h3>Forecasts Available Summary</h3>
          <div class="summary-grid">
            <div class="summary-card"><div class="meta">Count</div><strong id="afCount">-</strong></div>
            <div class="summary-card"><div class="meta">Topics</div><strong id="afTopics">-</strong></div>
            <div class="summary-card"><div class="meta">Dates</div><strong id="afDates">-</strong></div>
          </div>
        </section>
      </div>
    </div>
  </div>

  <script>
    const STORAGE_KEY = 'pvTestConsoleState';
    let lastJson = {};
    let loadedSpecs = [];
    let pollTimer = null;
    let latestPredictCache = [];
    let latestPredictRuntime = [];

    const defaults = {
      baseUrl: window.location.origin,
      date: new Date().toISOString().slice(0, 10),
      topic: '',
      stepMinutes: '15',
      days: '3',
      forecastTopic: '',
      dateFrom: '',
      dateTo: '',
    };

    const state = { ...defaults, ...JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}') };
    const setStatusPill = (ok) => {
      const status = document.getElementById('backendStatus');
      status.className = `status-pill ${ok ? 'status-ok' : 'status-error'}`;
      status.textContent = ok ? 'Backend: online' : 'Backend: offline';
    };

    const weatherChart = new Chart(document.getElementById('weatherChart'), {
      type: 'line',
      data: { labels: [], datasets: [
        { label: 'Temp °C', data: [], borderColor: '#dc2626', yAxisID: 'y' },
        { label: 'Cloud %', data: [], borderColor: '#0ea5e9', yAxisID: 'y1' }
      ] },
      options: { responsive: true, maintainAspectRatio: false, scales: { y: { type: 'linear' }, y1: { type: 'linear', position: 'right', min: 0, max: 100 } } }
    });

    const clearSkyChart = new Chart(document.getElementById('clearSkyChart'), {
      type: 'line',
      data: { labels: [], datasets: [{ label: 'Clear-sky radiation (W/m²)', data: [], borderColor: '#1f77b4', tension: 0.2 }] },
      options: { responsive: true, maintainAspectRatio: false }
    });

    const powerChart = new Chart(document.getElementById('powerChart'), {
      type: 'line',
      data: { labels: [], datasets: [
        { label: 'Cache', data: [], borderColor: '#16a34a' },
        { label: 'Runtime', data: [], borderColor: '#9333ea' }
      ] },
      options: { responsive: true, maintainAspectRatio: false }
    });

    const bindState = () => {
      document.getElementById('baseUrl').value = state.baseUrl;
      document.getElementById('dateInput').value = state.date;
      document.getElementById('stepMinutes').value = state.stepMinutes;
      document.getElementById('daysInput').value = state.days;
      document.getElementById('forecastTopic').value = state.forecastTopic;
      document.getElementById('dateFrom').value = state.dateFrom;
      document.getElementById('dateTo').value = state.dateTo;
    };
    bindState();

    const persistState = () => {
      state.baseUrl = document.getElementById('baseUrl').value;
      state.date = document.getElementById('dateInput').value;
      state.topic = document.getElementById('topicSelect').value;
      state.stepMinutes = document.getElementById('stepMinutes').value;
      state.days = document.getElementById('daysInput').value;
      state.forecastTopic = document.getElementById('forecastTopic').value;
      state.dateFrom = document.getElementById('dateFrom').value;
      state.dateTo = document.getElementById('dateTo').value;
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    };

    const logAction = (text, tone = 'ok') => {
      const div = document.createElement('div');
      div.className = 'log-item';
      div.innerHTML = `<span class="${tone}">●</span> ${new Date().toLocaleTimeString()} — ${text}`;
      const log = document.getElementById('actionLog');
      log.prepend(div);
    };

    const setEndpointStatus = (id, message, tone) => {
      const el = document.getElementById(id);
      el.className = `endpoint-status ${tone}`;
      el.textContent = message;
    };

    const setJson = (obj) => {
      lastJson = obj;
      document.getElementById('jsonPreview').textContent = JSON.stringify(obj, null, 2);
    };

    const safeJsonParse = (value) => {
      if (!value) return null;
      try {
        return JSON.parse(value);
      } catch (err) {
        return { _parseError: err.message, _raw: String(value).slice(0, 1000) };
      }
    };

    const request = async (path, options = {}) => {
      persistState();
      const base = document.getElementById('baseUrl').value.replace(/\\/$/, '');
      const started = performance.now();
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 30000);
      try {
        const res = await fetch(`${base}${path}`, { ...options, signal: controller.signal });
        const duration = Math.round(performance.now() - started);
        const text = await res.text();
        const json = safeJsonParse(text);
        const requestBody = safeJsonParse(options.body);
        document.getElementById('httpStatus').textContent = res.status;
        document.getElementById('httpLatency').textContent = `${duration} ms`;
        setJson({
          endpoint: path,
          request: requestBody,
          response: json,
          response_meta: { status: res.status, content_type: res.headers.get('content-type') || '' },
        });
        if (!res.ok) {
          const detail = json && typeof json === 'object' ? json.detail : null;
          throw new Error(detail || `HTTP ${res.status}`);
        }
        logAction(`${path} OK (${duration}ms)`, 'ok');
        return { data: json || {}, status: res.status, duration };
      } catch (err) {
        const duration = Math.round(performance.now() - started);
        document.getElementById('httpLatency').textContent = `${duration} ms`;
        document.getElementById('httpStatus').textContent = 'error';
        const message = err.name === 'AbortError' ? 'Timeout (30s)' : err.message;
        logAction(`${path} FAIL: ${message}`, 'err');
        setJson({ endpoint: path, error: message, request: safeJsonParse(options.body) });
        throw err;
      } finally {
        clearTimeout(timeout);
      }
    };

    const getSelectedTopic = () => {
      const topic = document.getElementById('topicSelect').value;
      if (!topic) {
        throw new Error('Сначала выберите topic');
      }
      return topic;
    };

    const renderWeather = (payload) => {
      const labels = payload.points.map((p) => (p.time || '').slice(11));
      weatherChart.data.labels = labels;
      weatherChart.data.datasets[0].data = payload.points.map((p) => p.temp_c);
      weatherChart.data.datasets[1].data = payload.points.map((p) => p.cloud);
      weatherChart.update();

      const table = document.getElementById('weatherTable');
      table.innerHTML = '<thead><tr><th>time</th><th>temp_c</th><th>cloud</th><th>source</th><th>status</th></tr></thead>' +
        '<tbody>' + payload.points.map((p) => `<tr><td>${p.time || ''}</td><td>${p.temp_c ?? ''}</td><td>${p.cloud ?? ''}</td><td>${payload.source}</td><td>${payload.status}</td></tr>`).join('') + '</tbody>';
    };

    const downsample = (arr, max = 200) => {
      if (arr.length <= max) return arr;
      const step = Math.ceil(arr.length / max);
      return arr.filter((_, idx) => idx % step === 0);
    };

    const renderPower = () => {
      const cache = downsample(latestPredictCache);
      const runtime = downsample(latestPredictRuntime);
      const labels = [...new Set([...cache.map((p) => p.x), ...runtime.map((p) => p.x)])].sort();
      const toMap = (items) => Object.fromEntries(items.map((p) => [p.x, p.y]));
      const cacheMap = toMap(cache);
      const runtimeMap = toMap(runtime);
      powerChart.data.labels = labels.map((x) => x.slice(11));
      powerChart.data.datasets[0].data = labels.map((x) => cacheMap[x] ?? null);
      powerChart.data.datasets[1].data = labels.map((x) => runtimeMap[x] ?? null);
      powerChart.update();

      const table = document.getElementById('powerTable');
      table.innerHTML = '<thead><tr><th>time</th><th>cache</th><th>runtime</th><th>delta</th><th>source(runtime)</th></tr></thead><tbody>' +
        labels.map((x) => {
          const c = cacheMap[x];
          const rItem = runtime.find((p) => p.x === x);
          const r = rItem ? rItem.y : null;
          const delta = (c != null && r != null) ? (r - c).toFixed(3) : '';
          return `<tr><td>${x}</td><td>${c ?? ''}</td><td>${r ?? ''}</td><td>${delta}</td><td>${rItem?.source || ''}</td></tr>`;
        }).join('') + '</tbody>';
    };

    const renderClearSky = (payload) => {
      clearSkyChart.data.labels = payload.points.map((p) => p.time.slice(11));
      clearSkyChart.data.datasets[0].data = payload.points.map((p) => p.clear_sky_radiation_w_m2);
      clearSkyChart.update();
    };

    const fillTopics = (topics) => {
      const sel = document.getElementById('topicSelect');
      sel.innerHTML = '<option value="">-- выберите topic --</option>' + topics.map((t) => `<option value="${t.tag}">${t.tag}</option>`).join('');
      if (state.topic && topics.some((t) => t.tag === state.topic)) {
        sel.value = state.topic;
      }
      persistState();
      toggleTopicButtons();
    };

    const toggleTopicButtons = () => {
      const disabled = !document.getElementById('topicSelect').value;
      ['weatherBtn', 'clearSkyBtn', 'predictCacheBtn', 'predictRuntimeBtn', 'compareBtn', 'startJobBtn'].forEach((id) => {
        document.getElementById(id).disabled = disabled;
      });
    };

    const loadTopics = async () => {
      const result = await request('/topics');
      fillTopics(result.data.topics || []);
      setEndpointStatus('weatherStatus', `Topics loaded: ${(result.data.topics || []).length}`, 'ok');
      return result.data.topics || [];
    };

    const loadTopicSpecs = async () => {
      const result = await request('/topics/specs');
      loadedSpecs = result.data.specs || [];
      logAction(`Specs loaded: ${loadedSpecs.length}`, 'ok');
      return loadedSpecs;
    };

    const getWeather = async () => {
      const topic = getSelectedTopic();
      const payload = { tag: topic, prediction_date: document.getElementById('dateInput').value };
      const result = await request('/weather_info', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
      });
      renderWeather(result.data);
      const tone = result.data.status === 'no_data' ? 'warn' : 'ok';
      setEndpointStatus('weatherStatus', `Weather: ${result.data.status}, source=${result.data.source}, points=${result.data.points.length}`, tone);
    };

    const getClearSky = async () => {
      const topic = getSelectedTopic();
      const payload = { tag: topic, date: document.getElementById('dateInput').value, step_minutes: Number(document.getElementById('stepMinutes').value) };
      const result = await request('/radiation/clear-sky', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
      });
      renderClearSky(result.data);
      setEndpointStatus('clearSkyStatus', `Clear-sky points=${result.data.points.length}, source=${result.data.source}`, 'ok');
    };

    const predict = async (runtime = false) => {
      const topic = getSelectedTopic();
      const payload = { prediction_date: document.getElementById('dateInput').value, topics: [topic] };
      const endpoint = runtime ? '/predict/runtime' : '/predict';
      const result = await request(endpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
      });
      const points = result.data.points[topic] || [];
      if (runtime) latestPredictRuntime = points;
      else latestPredictCache = points;
      renderPower();
      const weatherSource = runtime && points[0] ? points[0].source : '-';
      setEndpointStatus('predictStatus', `${result.data.mode}: points=${points.length}, runtime weather source=${weatherSource}`, 'ok');
    };

    const startHistoryJob = async () => {
      const payload = { days: Number(document.getElementById('daysInput').value) };
      const result = await request('/jobs/generate-history', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
      });
      document.getElementById('jobIdInput').value = result.data.job.id;
      setEndpointStatus('jobStatus', `Job ${result.data.job.id} -> ${result.data.job.state}`, 'ok');
    };

    const checkJob = async () => {
      const jobId = document.getElementById('jobIdInput').value;
      if (!jobId) throw new Error('Введите job_id');
      const result = await request(`/jobs/${jobId}`);
      const state = result.data.state;
      const tone = state === 'failed' ? 'err' : (state === 'completed' ? 'ok' : 'warn');
      setEndpointStatus('jobStatus', `Job ${jobId}: ${state}${result.data.error ? ` (${result.data.error})` : ''}`, tone);
      if (['completed', 'failed'].includes(state)) {
        if (pollTimer) clearInterval(pollTimer);
      }
      return state;
    };

    const getAvailableForecasts = async () => {
      const params = new URLSearchParams();
      const topic = document.getElementById('forecastTopic').value;
      const dateFrom = document.getElementById('dateFrom').value;
      const dateTo = document.getElementById('dateTo').value;
      if (topic) params.set('topic', topic);
      if (dateFrom) params.set('date_from', dateFrom);
      if (dateTo) params.set('date_to', dateTo);
      const query = params.toString() ? `?${params.toString()}` : '';
      const result = await request(`/forecasts/available${query}`);
      document.getElementById('afCount').textContent = result.data.count;
      document.getElementById('afTopics').textContent = result.data.topics.length;
      document.getElementById('afDates').textContent = result.data.dates.length;
    };

    const runSmokeFlow = async () => {
      try {
        const topics = await loadTopics();
        if (!document.getElementById('topicSelect').value && topics[0]) {
          document.getElementById('topicSelect').value = topics[0].tag;
        }
        toggleTopicButtons();
        await getWeather();
        await getClearSky();
        await predict(false);
        await predict(true);
        logAction('Smoke flow completed', 'ok');
      } catch (err) {
        logAction(`Smoke flow failed: ${err.message}`, 'err');
      }
    };

    document.getElementById('loadTopicsBtn').addEventListener('click', () => loadTopics().catch((e) => alert(e.message)));
    document.getElementById('loadSpecsBtn').addEventListener('click', () => loadTopicSpecs().catch((e) => alert(e.message)));
    document.getElementById('weatherBtn').addEventListener('click', () => getWeather().catch((e) => alert(e.message)));
    document.getElementById('clearSkyBtn').addEventListener('click', () => getClearSky().catch((e) => alert(e.message)));
    document.getElementById('predictCacheBtn').addEventListener('click', () => predict(false).catch((e) => alert(e.message)));
    document.getElementById('predictRuntimeBtn').addEventListener('click', () => predict(true).catch((e) => alert(e.message)));
    document.getElementById('compareBtn').addEventListener('click', async () => {
      try {
        await predict(false);
        await predict(true);
      } catch (e) {
        alert(e.message);
      }
    });
    document.getElementById('startJobBtn').addEventListener('click', () => startHistoryJob().catch((e) => alert(e.message)));
    document.getElementById('checkJobBtn').addEventListener('click', () => checkJob().catch((e) => alert(e.message)));
    document.getElementById('availableBtn').addEventListener('click', () => getAvailableForecasts().catch((e) => alert(e.message)));
    document.getElementById('smokeBtn').addEventListener('click', runSmokeFlow);
    document.getElementById('topicSelect').addEventListener('change', () => { persistState(); toggleTopicButtons(); document.getElementById('forecastTopic').value = document.getElementById('topicSelect').value; });
    ['baseUrl', 'dateInput', 'stepMinutes', 'daysInput', 'forecastTopic', 'dateFrom', 'dateTo'].forEach((id) => {
      document.getElementById(id).addEventListener('change', persistState);
    });

    document.getElementById('copyJsonBtn').addEventListener('click', async () => {
      await navigator.clipboard.writeText(JSON.stringify(lastJson, null, 2));
      logAction('JSON copied', 'ok');
    });

    document.getElementById('resetSessionBtn').addEventListener('click', () => {
      localStorage.removeItem(STORAGE_KEY);
      window.location.reload();
    });

    document.getElementById('autoPollToggle').addEventListener('change', (e) => {
      if (e.target.checked) {
        pollTimer = setInterval(() => checkJob().catch(() => {}), 3000);
      } else if (pollTimer) {
        clearInterval(pollTimer);
      }
    });

    (async () => {
      try {
        await request('/docs');
        setStatusPill(true);
      } catch (_) {
        setStatusPill(false);
      }
      try {
        await loadTopics();
      } catch (_) {
        toggleTopicButtons();
      }
    })();
  </script>
</body>
</html>
        """
    )
