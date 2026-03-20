import logging
import threading
import time

from config import load_settings
from forecast_db import run_migrations
from jobs.generate_forecasts import run_fixation, run_future, run_history

logger = logging.getLogger(__name__)
settings = load_settings()


def _run_loop(name: str, func, interval_seconds: int) -> None:
    """Universal loop: run func, sleep interval_seconds, repeat."""
    while True:
        try:
            logger.info("[%s] starting", name)
            func()
            logger.info("[%s] completed, sleeping %ds", name, interval_seconds)
        except Exception:
            logger.exception("[%s] failed", name)
        time.sleep(interval_seconds)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run_migrations()

    # Process 1: fill history gaps on startup
    run_history()

    # Process 2: refresh forecast every N minutes
    forecast_thread = threading.Thread(
        target=_run_loop,
        args=("forecast", run_future, settings.forecast_refresh_minutes * 60),
        daemon=True,
    )

    # Process 3: fixation of yesterday's fact — once per day
    fixation_thread = threading.Thread(
        target=_run_loop,
        args=("fixation", run_fixation, 86400),
        daemon=True,
    )

    forecast_thread.start()
    fixation_thread.start()

    # Main thread waits (daemon threads die with process)
    forecast_thread.join()


if __name__ == "__main__":
    main()
