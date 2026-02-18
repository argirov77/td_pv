from __future__ import annotations

from datetime import datetime
from typing import Iterable, Sequence

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
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (topic, ts)
    ) PARTITION BY RANGE (ts);

    CREATE INDEX IF NOT EXISTS idx_pv_forecast_topic_ts
    ON pv_forecast_points (topic, ts);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


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


def bulk_upsert_points(rows: Sequence[tuple[str, datetime, float]]) -> None:
    if not rows:
        return

    sql = """
    INSERT INTO pv_forecast_points (topic, ts, power)
    VALUES %s
    ON CONFLICT (topic, ts)
    DO UPDATE SET
      power = EXCLUDED.power,
      created_at = now()
    """

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        raw_conn.commit()
    finally:
        raw_conn.close()


def select_points(topics: Sequence[str], start_ts: datetime, end_ts: datetime) -> dict[str, list[dict[str, float]]]:
    if not topics:
        return {}

    result: dict[str, list[dict[str, float]]] = {topic: [] for topic in topics}
    query = text(
        """
        SELECT topic, ts, power
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
        })

    return result
