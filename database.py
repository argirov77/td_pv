import logging
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection parameters for the archive_main database where tag_specification lives
DB_URI_SPEC = "postgresql://postgres:smartgrid@172.31.168.2/archive_main"
engine_spec = create_engine(DB_URI_SPEC)

def get_tag_specification(topic: str) -> dict:
    """
    Fetch a record from the tag_specification table by the given topic.
    The table must contain a column named "sm_user_object_id" (used to link to weather_data.user_object_id).
    Returns a dict of the row’s fields, or None if not found.
    """
    try:
        with engine_spec.connect() as conn:
            query = text("SELECT * FROM tag_specification WHERE tag = :topic LIMIT 1")
            df = pd.read_sql(query, conn, params={"topic": topic})

        if df.empty:
            logger.warning(f"No specification found for topic '{topic}'.")
            return None

        spec = df.iloc[0].to_dict()

        if not spec.get("sm_user_object_id"):
            logger.warning(f"'sm_user_object_id' is missing for topic '{topic}'.")

        logger.info(f"Specification retrieved for topic '{topic}': {spec}")
        return spec

    except Exception as e:
        logger.error(f"Error retrieving specification for topic '{topic}': {e}")
        return None
