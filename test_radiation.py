# test_radiation.py

import pvlib
import pandas as pd
from pvlib.location import Location
import numpy as np
from radiation import calculate_panel_power

def debug_calculation(latitude, longitude, dt, panel_area, panel_tilt, panel_azimuth, efficiency, tz):
    # Привръщаме времето към timezone-aware
    time = pd.Timestamp(dt)
    if time.tzinfo is None:
        time = time.tz_localize(tz)
    print(f"Време за изчисление: {time}\n")
    
    # Опакваме времето в DatetimeIndex
    time_index = pd.DatetimeIndex([time])
    
    # Създаваме обект Location с часова зона
    site = Location(latitude, longitude, tz=tz)
    
    # Получаваме позицията на Слънцето за DatetimeIndex
    solpos = site.get_solarposition(time_index)
    print("Позиция на Слънцето:")
    print(solpos, "\n")
    
    # Получаваме clearsky данни за DatetimeIndex
    try:
        clearsky = site.get_clearsky(time_index)
        print("Clearsky данни:")
        print(clearsky, "\n")
    except Exception as e:
        print("Грешка при получаване на clearsky:", e)
        return

    # Изчисляваме падналото излъчване върху наклонена повърхност (POA)
    try:
        poa = pvlib.irradiance.get_total_irradiance(
            surface_tilt=panel_tilt,
            surface_azimuth=panel_azimuth,
            solar_zenith=solpos['zenith'],
            solar_azimuth=solpos['azimuth'],
            dni=clearsky['dni'],
            ghi=clearsky['ghi'],
            dhi=clearsky['dhi']
        )['poa_global']
        print("POA (паднало излъчване върху наклонена повърхност):")
        print(poa, "\n")
    except Exception as e:
        print("Грешка при изчисляване на POA:", e)
    
    # Изчисляваме мощността на панела с помощта на нашата функция
    power = calculate_panel_power(latitude, longitude, dt, panel_area, panel_tilt, panel_azimuth, efficiency, tz)
    print(f"Изчислена мощност на панела: {power:.2f} W")
    return power

if __name__ == "__main__":
    # Примерни параметри за Москва
    latitude = 55.7558         # Географска ширина
    longitude = 37.6176        # Географска дължина
    dt = "2025-03-10 12:00:00"   # Време за изчисление (обяд)
    panel_area = 10.0          # Площ на панела в m²
    panel_tilt = 30.0          # Ъгъл на наклон на панела в градуси
    panel_azimuth = 180.0      # Азимут на панела (180° - южно)
    efficiency = 0.18          # Ефективност на панела (18%)
    tz = "Europe/Moscow"       # Часова зона

    debug_calculation(latitude, longitude, dt, panel_area, panel_tilt, panel_azimuth, efficiency, tz)
