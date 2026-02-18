import time

from sqlalchemy import text

from config import load_settings
from forecast_db import engine, run_migrations
from jobs.generate_forecasts import run_future, run_history

settings = load_settings()


def _cache_has_rows() -> bool:
    with engine.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM pv_forecast_points")).scalar()
    return bool(count)


def main() -> None:
    run_migrations()
    if not _cache_has_rows():
        run_history()
    while True:
        run_future()
        time.sleep(settings.forecast_refresh_hours * 3600)


if __name__ == "__main__":
    main()
