import time

from config import load_settings
from jobs.generate_forecasts import run_future

settings = load_settings()


def main() -> None:
    while True:
        run_future()
        time.sleep(settings.forecast_refresh_hours * 3600)


if __name__ == "__main__":
    main()
