import logging
import os
from typing import Any

from sqlalchemy import bindparam, create_engine, text

logger = logging.getLogger(__name__)

DB_URI = os.getenv("FORECAST_DB_URI", "postgresql://postgres:smartgrid@172.31.168.2/solar_db")
engine = create_engine(DB_URI)


def select_points(topics: list[str], start_ts: str, end_ts: str) -> list[dict[str, Any]]:
    """Връща точки за topic-и в интервала [start_ts, end_ts)."""
    if not topics:
        return []

    sql = text(
        """
        SELECT topic, ts AS x, value AS y
        FROM forecast_points
        WHERE topic IN :topics
          AND ts >= :start_ts
          AND ts < :end_ts
        ORDER BY topic, ts
        """
    ).bindparams(bindparam("topics", expanding=True))

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sql,
                {
                    "topics": topics,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                },
            ).mappings()
            return [dict(row) for row in rows]
    except Exception:
        logger.exception("Неуспешно извличане на точки от forecast_points")
        return []
