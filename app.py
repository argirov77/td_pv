from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from config import load_settings
from forecast_db import run_migrations, select_points
from forecast_pipeline import (
    compute_forecast,
    get_job_status,
    get_topic_spec,
    get_weather,
    list_topic_specs,
    start_history_job,
)
from radiation import calculate_panel_irradiance

settings = load_settings()
app = FastAPI()


class PredictRequest(BaseModel):
    prediction_date: str
    topics: list[str] = Field(default_factory=list)


class HistoryJobRequest(BaseModel):
    topics: list[str] = Field(default_factory=list)
    days_back: int = 365
    end_date: str
    write: bool = True
    max_days_per_run: int = 7
    max_topics_per_run: int = 50


class ClearSkyRequest(BaseModel):
    topic: str
    date: str
    step_minutes: int = 15


class WeatherRequest(BaseModel):
    topic: str
    date: str
    source: str = "auto"


class ComputeRequest(BaseModel):
    topic: str
    date: str
    write: bool = False
    weather_source: str = "auto"


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
        raise HTTPException(status_code=400, detail="invalid prediction_date format YYYY-MM-DD")
    return select_points(request.topics, day_start, day_start + timedelta(days=1))


@app.get("/debug/spec/topics")
def debug_spec_topics(limit: int = 200, like: str | None = None):
    return list_topic_specs(limit=limit, like=like)


@app.get("/debug/forecast/coverage")
def debug_forecast_coverage(topic: str, date: str):
    from forecast_db import engine as forecast_engine

    day_start = datetime.strptime(date, "%Y-%m-%d")
    day_end = day_start + timedelta(days=1)
    with forecast_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT count(*) AS points_count,
                       min(ts) AS min_ts,
                       max(ts) AS max_ts
                FROM pv_forecast_points
                WHERE topic=:topic AND ts>=:day_start AND ts<:day_end
                """
            ),
            {"topic": topic, "day_start": day_start, "day_end": day_end},
        ).mappings().first()

    points_count = int(row["points_count"] or 0)
    return {
        "topic": topic,
        "date": date,
        "exists": points_count > 0,
        "points_count": points_count,
        "min_ts": row["min_ts"].strftime("%Y-%m-%d %H:%M") if row["min_ts"] else None,
        "max_ts": row["max_ts"].strftime("%Y-%m-%d %H:%M") if row["max_ts"] else None,
        "gaps": max(0, 96 - points_count) if points_count > 0 else 0,
    }


@app.get("/debug/forecast/points")
def debug_forecast_points(topic: str, date: str, limit: int = 200):
    from forecast_db import engine as forecast_engine

    day_start = datetime.strptime(date, "%Y-%m-%d")
    day_end = day_start + timedelta(days=1)
    with forecast_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT ts, power FROM pv_forecast_points
                WHERE topic=:topic AND ts>=:day_start AND ts<:day_end
                ORDER BY ts LIMIT :limit
                """
            ),
            {"topic": topic, "day_start": day_start, "day_end": day_end, "limit": limit},
        ).mappings().all()

    return {
        "topic": topic,
        "date": date,
        "points": [{"x": r["ts"].strftime("%Y-%m-%d %H:%M"), "y": float(r["power"])} for r in rows],
        "limit": limit,
    }


@app.post("/debug/jobs/generate-history")
def debug_generate_history(req: HistoryJobRequest):
    end_date = datetime.strptime(req.end_date, "%Y-%m-%d").date()
    return start_history_job(
        topics=req.topics,
        days_back=req.days_back,
        end_date=end_date,
        write=req.write,
        max_days_per_run=req.max_days_per_run,
        max_topics_per_run=req.max_topics_per_run,
    )


@app.get("/debug/jobs/status")
def debug_job_status(job_id: str):
    return get_job_status(job_id)


@app.post("/debug/physics/clearsky-irradiance")
def debug_clearsky(req: ClearSkyRequest):
    from datetime import timedelta
    from pvlib.location import Location
    import pandas as pd

    spec = get_topic_spec(req.topic)
    if not spec or spec.lat is None or spec.lon is None:
        raise HTTPException(status_code=404, detail="topic missing coords")

    start = datetime.strptime(req.date, "%Y-%m-%d")
    times = pd.date_range(start=start, periods=(24 * 60) // req.step_minutes, freq=f"{req.step_minutes}min", tz="Europe/Nicosia")
    site = Location(spec.lat, spec.lon, tz="Europe/Nicosia")
    solpos = site.get_solarposition(times)
    clearsky = site.get_clearsky(times)
    poa = __import__('pvlib').irradiance.get_total_irradiance(
        surface_tilt=spec.tilt,
        surface_azimuth=spec.azimuth,
        solar_zenith=solpos["zenith"],
        solar_azimuth=solpos["azimuth"],
        dni=clearsky["dni"],
        ghi=clearsky["ghi"],
        dhi=clearsky["dhi"],
    )

    points = []
    for ts in times:
        idx = times.get_loc(ts)
        points.append(
            {
                "x": ts.strftime("%Y-%m-%d %H:%M"),
                "ghi": float(clearsky["ghi"].iloc[idx]),
                "dni": float(clearsky["dni"].iloc[idx]),
                "dhi": float(clearsky["dhi"].iloc[idx]),
                "poa_global": float(poa["poa_global"].iloc[idx]),
            }
        )

    return {
        "meta": {"lat": spec.lat, "lon": spec.lon, "tilt": spec.tilt, "azimuth": spec.azimuth},
        "points": points,
    }


@app.post("/debug/weather")
def debug_weather(req: WeatherRequest):
    d = datetime.strptime(req.date, "%Y-%m-%d").date()
    return get_weather(req.topic, d, req.source)


@app.post("/debug/predict/compute")
def debug_compute(req: ComputeRequest):
    d = datetime.strptime(req.date, "%Y-%m-%d").date()
    return compute_forecast(req.topic, d, weather_source=req.weather_source, write=req.write)


@app.get("/test-ui", response_class=HTMLResponse)
def test_ui():
    return """
<!doctype html><html><body><h2>PV Pipeline Test UI</h2>
<p>Stores inputs in localStorage.</p>
<div>
<h3>1) Topics with specifications list</h3>
<input id='like' placeholder='like'><button onclick='loadTopics()'>Load</button><pre id='topics'></pre>
</div>
<div><h3>2) Available cached forecasts in forecast DB</h3>
<input id='c_topic' placeholder='topic'><input id='c_date' placeholder='YYYY-MM-DD'><button onclick='coverage()'>Check Coverage</button><pre id='coverage'></pre>
<button onclick='points()'>Load Points</button><pre id='points'></pre></div>
<div><h3>3) Generate history forecasts for last 365 days</h3>
<textarea id='h_topics' placeholder='one topic per line'></textarea><br/><button onclick='genHistory()'>Generate</button><pre id='history'></pre></div>
<div><h3>4) Clear-sky irradiance (pvlib)</h3>
<input id='cs_topic' placeholder='topic'><input id='cs_date' placeholder='YYYY-MM-DD'><button onclick='clearsky()'>Run</button><pre id='cs'></pre></div>
<div><h3>5) Weather diagnostics by topic/date + source used</h3>
<input id='w_topic' placeholder='topic'><input id='w_date' placeholder='YYYY-MM-DD'><select id='w_source'><option>auto</option><option>archive_db</option><option>weather_api</option></select><button onclick='weather()'>Run</button><div id='w_src'></div><pre id='w'></pre></div>
<div><h3>6) Full forecast compute by topic/date + weather source used</h3>
<input id='p_topic' placeholder='topic'><input id='p_date' placeholder='YYYY-MM-DD'><select id='p_source'><option>auto</option><option>archive_db</option><option>weather_api</option></select><label><input id='p_write' type='checkbox'>write cache</label><button onclick='compute()'>Run</button><div id='p_src'></div><canvas id='chart' width='700' height='200' style='border:1px solid #ccc'></canvas><pre id='p'></pre></div>
<script>
function save(k,v){localStorage.setItem(k,v)}; function load(k){return localStorage.getItem(k)||''}
['c_topic','c_date','cs_topic','cs_date','w_topic','w_date','p_topic','p_date','like'].forEach(id=>document.getElementById(id).value=load(id));
function persist(){['c_topic','c_date','cs_topic','cs_date','w_topic','w_date','p_topic','p_date','like'].forEach(id=>save(id,document.getElementById(id).value)); save('h_topics',document.getElementById('h_topics').value);}
document.getElementById('h_topics').value=load('h_topics');
async function j(url,opt){persist(); const r=await fetch(url,opt); return await r.json();}
async function loadTopics(){document.getElementById('topics').textContent=JSON.stringify(await j('/debug/spec/topics?limit=200&like='+encodeURIComponent(document.getElementById('like').value)),null,2)}
async function coverage(){document.getElementById('coverage').textContent=JSON.stringify(await j(`/debug/forecast/coverage?topic=${encodeURIComponent(c_topic.value)}&date=${c_date.value}`),null,2)}
async function points(){document.getElementById('points').textContent=JSON.stringify(await j(`/debug/forecast/points?topic=${encodeURIComponent(c_topic.value)}&date=${c_date.value}&limit=200`),null,2)}
async function genHistory(){const topics=h_topics.value.split('\n').map(x=>x.trim()).filter(Boolean);const res=await j('/debug/jobs/generate-history',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({topics,days_back:365,end_date:new Date().toISOString().slice(0,10),write:true,max_days_per_run:7,max_topics_per_run:50})});history.textContent=JSON.stringify(res,null,2)}
async function clearsky(){cs.textContent=JSON.stringify(await j('/debug/physics/clearsky-irradiance',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({topic:cs_topic.value,date:cs_date.value,step_minutes:15})}),null,2)}
async function weather(){const res=await j('/debug/weather',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({topic:w_topic.value,date:w_date.value,source:w_source.value})});w_src.innerHTML=`<b>source:</b> ${res.weather_source_used} <b>reason:</b> ${res.weather_source_reason}`;w.textContent=JSON.stringify(res,null,2)}
async function compute(){const res=await j('/debug/predict/compute',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({topic:p_topic.value,date:p_date.value,weather_source:p_source.value,write:p_write.checked})});p_src.innerHTML=`<b>source:</b> ${res.weather_source_used} <b>reason:</b> ${res.weather_source_reason}`;p.textContent=JSON.stringify(res,null,2);draw(res.points||[])}
function draw(points){const c=document.getElementById('chart');const x=c.getContext('2d');x.clearRect(0,0,c.width,c.height);if(!points.length)return;const ys=points.map(p=>p.y);const min=Math.min(...ys),max=Math.max(...ys)||1;x.beginPath();points.forEach((p,i)=>{const px=i*(c.width/(points.length-1));const py=c.height-((p.y-min)/(max-min||1))*c.height; if(i===0)x.moveTo(px,py); else x.lineTo(px,py)});x.strokeStyle='#2d6cdf';x.stroke();}
</script></body></html>
"""
