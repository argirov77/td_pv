import pvlib
import pandas as pd
from pvlib.location import Location
import numpy as np

def calculate_panel_irradiance(latitude: float, longitude: float, dt, panel_tilt: float, panel_azimuth: float, tz: str = "Europe/Nicosia") -> float:
    """
    Изчислява наклонено излъчване (POA) в W/m² за дадени координати, време, ъгъл на наклон и азимут.
    Ако слънцето е под хоризонта, връща 0.
    Ако panel_azimuth е None, се използва стойност по подразбиране (180.0).
    """
    try:
        # Ако стойността panel_azimuth липсва, използваме стойност по подразбиране.
        if panel_azimuth is None:
            panel_azimuth = 180.0
        
        # Привръщаме времето към timezone-aware
        time = pd.Timestamp(dt)
        if time.tzinfo is None:
            time = time.tz_localize(tz)
        
        # Опаковаме времето в DatetimeIndex
        time_index = pd.DatetimeIndex([time])
        
        # Създаваме обект Location
        site = Location(latitude, longitude, tz=tz)
        
        # Получаваме позицията на слънцето за DatetimeIndex
        solpos = site.get_solarposition(time_index)
        
        # Ако слънцето е под хоризонта – връщаме 0
        if solpos.empty or solpos['apparent_zenith'].iloc[0] >= 90:
            return 0.0
        
        # Получаваме clearsky данни за DatetimeIndex
        clearsky = site.get_clearsky(time_index)
        
        # Изчисляваме наклоненото излъчване (POA)
        poa_irradiance = pvlib.irradiance.get_total_irradiance(
            surface_tilt=panel_tilt,
            surface_azimuth=panel_azimuth,
            solar_zenith=solpos['zenith'].values,
            solar_azimuth=solpos['azimuth'].values,
            dni=clearsky['dni'].values,
            ghi=clearsky['ghi'].values,
            dhi=clearsky['dhi'].values
        )['poa_global']
        
        value = float(poa_irradiance[0])
        if value is None or np.isnan(value):
            return 0.0
        return value
    except Exception as e:
        print("Грешка в calculate_panel_irradiance:", e)
        return 0.0
