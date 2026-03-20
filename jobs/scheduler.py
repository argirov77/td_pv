import logging
import threading
import time
from datetime import datetime, timezone

from config import load_settings
from forecast_db import run_migrations
from jobs.generate_forecasts import run_fixation, run_future, run_history

logger = logging.getLogger(__name__)
settings = load_settings()


def _run_loop(name: str, func, interval_seconds: int) -> None:
    while True:
        try:
            logger.info("[%s] starting", name)
            func()
            logger.info("[%s] completed, sleeping %ds", name, interval_seconds)
        except Exception:
            logger.exception("[%s] failed", name)
        time.sleep(interval_seconds)


def _seconds_until_hour(target_hour_utc: int) -> int:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=target_hour_utc, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target.replace(day=target.day + 1)
    return int((target - now).total_seconds())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    run_migrations()

    # Process 3: fill history gaps on startup
    logger.info("checking history gaps")
    run_history()

    # Process 1: forecast refresh every N minutes
    forecast_thread = threading.Thread(
        target=_run_loop,
        args=("forecast", run_future, settings.forecast_refresh_minutes * 60),
        daemon=True,
    )

    # Process 2: daily fact fixation at configured hour
    def _fixation_with_initial_wait():
        wait = _seconds_until_hour(settings.fact_fixation_hour_utc)
        logger.info("[fixation] waiting %ds until %02d:00 UTC", wait, settings.fact_fixation_hour_utc)
        time.sleep(wait)
        _run_loop("fixation", run_fixation, 86400)

    fixation_thread = threading.Thread(
        target=_fixation_with_initial_wait,
        daemon=True,
    )

    forecast_thread.start()
    fixation_thread.start()

    forecast_thread.join()


if __name__ == "__main__":
    main()
