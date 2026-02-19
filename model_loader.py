import os
from pathlib import Path
import joblib
import logging
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



def _resolve_model_dir() -> str:
    env_model_dir = os.getenv("MODEL_DIR")
    if env_model_dir:
        return env_model_dir

    project_model_dir = Path(__file__).resolve().parent / "Model"
    return str(project_model_dir)


MODEL_DIR = _resolve_model_dir()

def load_model(tag: str):
    """
    Зарежда модел за машинно обучение по даден tag.
    Ако моделът с име {tag}_model.pkl (или tag, ако вече има разширение .pkl) не е намерен,
    се използва модел с име "P0063H01_E001_model.pkl".
    """
    if tag.endswith(".pkl"):
        model_file = tag
    else:
        model_file = f"{tag}_model.pkl"

    model_path = os.path.join(MODEL_DIR, model_file)
    logger.debug(f"[load_model] Търсене на файл с модел: {model_path}")
    logger.debug(f"[load_model] MODEL_DIR = {MODEL_DIR!r}")
    logger.debug(f"[load_model] model_file = {model_file!r}")
    logger.debug(f"[load_model] model_path = {model_path!r}")
    logger.debug(f"[load_model] os.path.exists(model_path) = {os.path.exists(model_path)}")
    
    if not os.path.exists(model_path):
        logger.error(f"[load_model] Файлът с модел не е намерен: {model_path}")
        fallback_model = "P0063H01_E001_model.pkl"
        fallback_path = os.path.join(MODEL_DIR, fallback_model)
        logger.debug(f"[load_model] Fallback model path: {fallback_path}")
        if os.path.exists(fallback_path):
            try:
                logger.debug(f"[load_model] Опит за зареждане на резервен модел: {fallback_path}")
                model = joblib.load(fallback_path)
                logger.info(f"[load_model] Резервен модел зареден успешно от: {fallback_path}")
                return model
            except Exception as e:
                logger.exception(f"[load_model] Грешка при зареждане на резервен модел: {e}")
                return None
        else:
            logger.error(f"[load_model] Резервният файл с модел не е намерен: {fallback_path}")
            return None
    try:
        logger.debug(f"[load_model] Опит за зареждане на модел от: {model_path}")
        model = joblib.load(model_path)
        logger.info(f"[load_model] Моделът е зареден успешно от: {model_path}")
        return model
    except ModuleNotFoundError as mnfe:
        logger.error(f"[load_model] ModuleNotFoundError: {mnfe}. Проверете дали scikit-learn е инсталиран.")
        return None
    except Exception as e:
        logger.exception(f"[load_model] Грешка при зареждане на модел '{tag}': {e}")
        return None

def predict_power(model, weather_sample: dict):
    """
    Генерира прогноза за мощност, използвайки модел (model) и речник с метеорологични признаци (weather_sample).
    
    Поддържат се 2 типа модели:
      - Модел с 2 признака: "radiation_w_m2_y" и "cloud".
      - Модел с 25 признака: подробен набор от признаци.
    
    Функцията определя очаквания брой признаци (чрез model.n_features_in_) и формира DataFrame.
    Връща предсказанието или None при грешка.
    """
    try:
        n_features = getattr(model, 'n_features_in_', None)
        logger.debug(f"[predict_power] model.n_features_in_ = {n_features}")

        if n_features == 2:
            rad_value = weather_sample.get("radiation_w_m2_y", 0.0)
            cloud_value = weather_sample.get("cloud", 0.0)
            
            input_data = pd.DataFrame({
                "radiation_w_m2_y": [rad_value],
                "cloud": [cloud_value]
            })
            logger.debug(f"[predict_power] DataFrame с 2 признака:\n{input_data}")
            prediction = model.predict(input_data)[0]
            logger.info(f"[predict_power] Прогноза с 2-признаков модел: {prediction}")
            return prediction

        elif n_features == 25:
            logger.debug(f"[predict_power] Получен weather_sample: {weather_sample}")
            features = [
                weather_sample.get("hour_local", 0.0),
                weather_sample.get("temp_c", 0.0),
                weather_sample.get("is_day", 0.0),
                weather_sample.get("condition_text", ""),
                weather_sample.get("wind_kph", 0.0),
                weather_sample.get("wind_degree", 0.0),
                weather_sample.get("wind_dir", ""),
                weather_sample.get("pressure_mb", 0.0),
                weather_sample.get("precip_mm", 0.0),
                weather_sample.get("snow_cm", 0.0),
                weather_sample.get("humidity", 0.0),
                weather_sample.get("cloud", 0.0),
                weather_sample.get("feelslike_c", 0.0),
                weather_sample.get("windchill_c", 0.0),
                weather_sample.get("heatindex_c", 0.0),
                weather_sample.get("dewpoint_c", 0.0),
                weather_sample.get("will_it_rain", 0.0),
                weather_sample.get("chance_of_rain", 0.0),
                weather_sample.get("will_it_snow", 0.0),
                weather_sample.get("chance_of_snow", 0.0),
                weather_sample.get("vis_km", 0.0),
                weather_sample.get("gust_kph", 0.0),
                weather_sample.get("uv", 0.0),
                weather_sample.get("hour_category", ""),
                weather_sample.get("solar_intensity_score", 0.0)
            ]
            
            columns = [
                "hour_local", "temp_c", "is_day", "condition_text", "wind_kph", "wind_degree",
                "wind_dir", "pressure_mb", "precip_mm", "snow_cm", "humidity", "cloud",
                "feelslike_c", "windchill_c", "heatindex_c", "dewpoint_c", "will_it_rain",
                "chance_of_rain", "will_it_snow", "chance_of_snow", "vis_km", "gust_kph", "uv",
                "hour_category", "solar_intensity_score"
            ]
            feature_df = pd.DataFrame([features], columns=columns)
            logger.debug(f"[predict_power] DataFrame с 25 признака:\n{feature_df}")
            prediction = model.predict(feature_df)[0]
            logger.info(f"[predict_power] Прогноза с 25-признаков модел: {prediction}")
            return prediction

        else:
            logger.error(f"[predict_power] Непознат брой признаци в модела: {n_features}")
            return None

    except Exception as e:
        logger.exception(f"[predict_power] Грешка при прогнозиране: {e}")
        return None
