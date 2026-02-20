from __future__ import annotations

from datetime import datetime
from typing import Sequence

from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

from config import load_settings

settings = load_settings()
engine = create_engine(settings.forecast_db_dsn, pool_pre_ping=True)


def run_migrations() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS pv_forecast_points (
        topic TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        power DOUBLE PRECISION NOT NULL,
        source TEXT NOT NULL DEFAULT 'unknown',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (topic, ts)
    ) PARTITION BY RANGE (ts);

    CREATE INDEX IF NOT EXISTS idx_pv_forecast_topic_ts
    ON pv_forecast_points (topic, ts);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text("ALTER TABLE pv_forecast_points ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown'"))


def _month_bounds(ts: datetime) -> tuple[datetime, datetime]:
    month_start = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    return month_start, next_month


def ensure_month_partitions(start_ts: datetime, end_ts: datetime) -> None:
    cursor = start_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    with engine.begin() as conn:
        while cursor <= end_ts:
            frm, to = _month_bounds(cursor)
            table_name = f"pv_forecast_points_{frm.year}_{frm.month:02d}"
            conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name}
                    PARTITION OF pv_forecast_points
                    FOR VALUES FROM (:frm) TO (:to);
                    """
                ),
                {"frm": frm, "to": to},
            )
            cursor = to


def delete_future(start_ts: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pv_forecast_points WHERE ts >= :start_ts"), {"start_ts": start_ts})


def bulk_upsert_points(rows: Sequence[tuple[str, datetime, float, str]]) -> None:
    if not rows:
        return

    sql = """
    INSERT INTO pv_forecast_points (topic, ts, power, source)
    VALUES %s
    ON CONFLICT (topic, ts)
    DO UPDATE SET
      power = EXCLUDED.power,
      source = EXCLUDED.source,
      created_at = now()
    """

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        raw_conn.commit()
    finally:
        raw_conn.close()


def select_points(topics: Sequence[str], start_ts: datetime, end_ts: datetime) -> dict[str, list[dict[str, float | str]]]:
    if not topics:
        return {}

    result: dict[str, list[dict[str, float | str]]] = {topic: [] for topic in topics}
    query = text(
        """
        SELECT topic, ts, power, source
        FROM pv_forecast_points
        WHERE topic = ANY(:topics)
          AND ts >= :start_ts
          AND ts < :end_ts
        ORDER BY topic, ts
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, {"topics": list(topics), "start_ts": start_ts, "end_ts": end_ts}).mappings().all()

    for row in rows:
        result[row["topic"]].append({
            "x": row["ts"].strftime("%Y-%m-%d %H:%M"),
            "y": float(row["power"]),
            "source": row["source"],
        })

    return result


def select_available_forecasts(
    topic: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> dict[str, object]:
    where_parts: list[str] = []
    params: dict[str, object] = {}

    if topic:
        where_parts.append("topic = :topic")
        params["topic"] = topic
    if date_from:
        where_parts.append("ts >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where_parts.append("ts < :date_to")
        params["date_to"] = date_to

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    count_query = text(f"SELECT COUNT(*) AS total FROM pv_forecast_points {where_clause}")
    topics_query = text(f"SELECT DISTINCT topic FROM pv_forecast_points {where_clause} ORDER BY topic")
    dates_query = text(
        f"""
        SELECT DISTINCT DATE(ts) AS day
        FROM pv_forecast_points
        {where_clause}
        ORDER BY day
        """
    )

    with engine.connect() as conn:
        total = int(conn.execute(count_query, params).scalar_one())
        found_topics = [row[0] for row in conn.execute(topics_query, params).all()]
        found_dates = [row[0].isoformat() for row in conn.execute(dates_query, params).all() if row[0] is not None]

    return {
        "count": total,
        "topics": found_topics,
        "dates": found_dates,
    }
