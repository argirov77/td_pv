import logging

import pandas as pd
from sqlalchemy import create_engine, text

from config import load_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = load_settings()
engine_spec = create_engine(settings.archive_db_dsn, pool_pre_ping=True)


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
