import argparse

from forecast_pipeline import precompute_future_all_topics
from forecast_db import run_migrations


def run_future() -> None:
    run_migrations()
    precompute_future_all_topics()


def run_history() -> None:
    # history is manual via /debug/jobs/generate-history
    return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["future", "history"], required=True)
    args = parser.parse_args()
    if args.mode == "future":
        run_future()
    else:
        run_history()


if __name__ == "__main__":
    main()
