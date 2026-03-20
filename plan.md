# План рефакторинга: Три процесса обновления кеша прогнозов

## Обзор изменений

Текущий `scheduler.py` с двумя режимами (one-shot `run_history` + цикл `run_future` каждые 3 часа) заменяется на три независимых процесса с чёткими зонами ответственности.

---

## Файл 1: `config.py`

**Изменения:** Добавить новые параметры, заменить `forecast_refresh_hours` на минуты.

```python
# Убрать:
forecast_refresh_hours: int  # default=3

# Добавить:
forecast_refresh_minutes: int      # default=30, env FORECAST_REFRESH_MINUTES
fact_fixation_hour_utc: int        # default=1,  env FACT_FIXATION_HOUR_UTC  (час запуска фиксации факта)
history_gap_max_days: int          # default=3,  env HISTORY_GAP_MAX_DAYS (макс. допустимый пропуск дней)
```

---

## Файл 2: `forecast_db.py`

**Изменения:** Добавить две новые функции для поддержки процессов 2 и 3.

### Новая функция: `find_missing_days(topic, start_date, end_date) -> list[date]`
- Запрашивает `SELECT DISTINCT DATE(ts) FROM pv_forecast_points WHERE topic = :topic AND ts >= :start AND ts < :end`
- Сравнивает с полным набором дат в диапазоне
- Возвращает список дат без данных ("дыры")

### Новая функция: `delete_day(topic, day: date)`
- `DELETE FROM pv_forecast_points WHERE topic = :topic AND ts >= :day AND ts < :day + 1`
- Используется перед записью факта, чтобы гарантировать полную замену прогноза на факт

---

## Файл 3: `jobs/generate_forecasts.py`

**Изменения:** Добавить новую функцию `run_fixation()`, рефакторить `run_history()`.

### Существующая функция `run_future()` — без изменений логики
Единственное изменение: вызывается чаще (каждые 30 мин вместо 3 часов). Сама функция не меняется.

### Новая функция: `run_fixation()`
```
Фиксация факта за вчерашний день:
1. yesterday = (utcnow() - 1 day).date()
2. Для каждого topic:
   a. Получить weather из архивных БД (weather_main2 → solar_db)
      - Вызов get_weather_for_date() — для прошлых дат автоматически
        попробует archive_db_new, потом archive_db, потом weather_api
   b. Если source == 'weather_api' — пропустить (нет архивных данных, оставляем прогноз)
   c. Если source in ('archive_db_new', 'archive_db'):
      - Построить строки через _build_rows_for_topic(topic, records, 'archive_db')
      - Upsert в кеш (ON CONFLICT перезапишет прогнозные значения)
3. Логирование: сколько topic обработано, сколько пропущено
```

### Рефакторинг `run_history()` — добавить обнаружение "дыр"
```
Новая логика:
1. Определить диапазон: (now - forecast_history_days) .. now
2. Для каждого topic:
   a. Вызвать find_missing_days(topic, start, end) из forecast_db
   b. Для каждого пропущенного дня:
      - Загрузить погоду, построить строки, upsert
3. Если дыр нет — ничего не делаем (быстрый выход)
```

Это делает `run_history()` идемпотентной — можно запускать многократно, она заполнит только пропуски.

### Обновить `main()` CLI
Добавить `--mode fixation` в argparse:
```python
parser.add_argument("--mode", choices=["future", "history", "fixation"], required=True)
```

---

## Файл 4: `jobs/scheduler.py`

**Изменения:** Полная переработка — три потока вместо одного цикла.

```python
import threading
import time
from datetime import datetime

from config import load_settings
from forecast_db import run_migrations
from jobs.generate_forecasts import run_future, run_history, run_fixation

settings = load_settings()


def _run_loop(name: str, func, interval_seconds: int):
    """Универсальный цикл: запустить func, подождать interval_seconds, повторить."""
    while True:
        try:
            logger.info(f"[{name}] starting")
            func()
            logger.info(f"[{name}] completed, sleeping {interval_seconds}s")
        except Exception:
            logger.exception(f"[{name}] failed")
        time.sleep(interval_seconds)


def main():
    run_migrations()

    # Процесс 1: история (при старте, заполнение дыр)
    run_history()

    # Процесс 2: обновление прогноза каждые N минут
    forecast_thread = threading.Thread(
        target=_run_loop,
        args=("forecast", run_future, settings.forecast_refresh_minutes * 60),
        daemon=True,
    )

    # Процесс 3: фиксация факта — раз в сутки
    fixation_thread = threading.Thread(
        target=_run_loop,
        args=("fixation", run_fixation, 86400),  # 24 часа
        daemon=True,
    )

    forecast_thread.start()
    fixation_thread.start()

    # Основной поток ждёт (daemon threads умрут с процессом)
    forecast_thread.join()
```

**Примечание по времени запуска фиксации:** В простом варианте фиксация запускается сразу при старте и потом раз в 24 часа. Для точного запуска в `fact_fixation_hour_utc` можно добавить `_wait_until_hour()` перед первым вызовом — это опционально.

---

## Файл 5: `app.py`

**Изменения:** Добавить эндпоинт для ручного запуска фиксации.

### Новый эндпоинт: `POST /jobs/fix-yesterday`
- Запускает `run_fixation()` в фоне (аналогично `/jobs/generate-history`)
- Использует тот же `HistoryJobService` для отслеживания статуса

---

## Порядок реализации

| Шаг | Файл | Что делаем |
|-----|------|------------|
| 1 | `config.py` | Новые параметры |
| 2 | `forecast_db.py` | `find_missing_days()` |
| 3 | `jobs/generate_forecasts.py` | `run_fixation()` + рефакторинг `run_history()` |
| 4 | `jobs/scheduler.py` | Три потока |
| 5 | `app.py` | Эндпоинт `/jobs/fix-yesterday` |
| 6 | Тесты | Проверить все три процесса |

---

## Что НЕ меняется

- `_build_rows_for_topic()` — без изменений
- `weather_service.py` — без изменений (уже правильно выбирает источник по дате)
- `weather_db.py`, `weather_api.py` — без изменений
- `radiation.py`, `production.py`, `model_loader.py` — без изменений
- Схема БД `pv_forecast_points` — без изменений (используем существующее поле `source`)
