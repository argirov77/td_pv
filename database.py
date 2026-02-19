import logging

import pandas as pd
from sqlalchemy import create_engine, text

from config import load_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = load_settings()
engine_spec = create_engine(settings.archive_db_dsn, pool_pre_ping=True)


class DatabaseReadError(RuntimeError):
    """Raised when we cannot read data from the archive DB."""


def get_tag_specification(topic: str) -> dict | None:
    try:
        with engine_spec.connect() as conn:
            query = text("SELECT * FROM tag_specification WHERE tag = :topic LIMIT 1")
            df = pd.read_sql(query, conn, params={"topic": topic})
        if df.empty:
            return None
        return df.iloc[0].to_dict()
    except Exception as exc:
        logger.error("Error retrieving specification for topic '%s': %s", topic, exc)
        return None


def get_all_topics() -> list[str]:
    try:
        with engine_spec.connect() as conn:
            rows = conn.execute(text("SELECT tag FROM tag_specification WHERE tag IS NOT NULL")).fetchall()
        return [row[0] for row in rows]
    except Exception as exc:
        logger.error("Error reading topics from tag_specification: %s", exc)
        return []


def get_all_topics_or_raise() -> list[str]:
    try:
        with engine_spec.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT tag
                    FROM tag_specification
                    WHERE tag IS NOT NULL
                    ORDER BY tag
                    """
                )
            ).fetchall()
        return [row[0] for row in rows]
    except Exception as exc:
        logger.error("Error reading topics from tag_specification: %s", exc)
        raise DatabaseReadError("failed to read topics") from exc


def get_all_topic_specifications_or_raise() -> list[dict]:
    try:
        with engine_spec.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        tag,
                        sm_user_object_id,
                        latitude,
                        longitude,
                        tilt,
                        azimuth,
                        module_length,
                        module_width,
                        module_efficiency,
                        total_panels
                    FROM tag_specification
                    WHERE tag IS NOT NULL
                    ORDER BY tag
                    """
                )
            ).mappings().all()
        return [dict(row) for row in rows]
    except Exception as exc:
        logger.error("Error reading topic specifications: %s", exc)
        raise DatabaseReadError("failed to read topic specifications") from exc
