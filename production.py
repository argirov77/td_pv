import math
from datetime import datetime

def production_correction(temp_c: float, cloud_cover: float) -> float:
    """
    Изчислява коригиращ коефициент, отчитащ влиянието на температурата и облачността.
    
    Параметри:
      temp_c (float): Температура на въздуха в °C.
      cloud_cover (float): Облачност в дялове (0 = ясно, 1 = пълно покритие).
    
    Използваме:
      - Температурен коефициент: -0.44%/°C с добавяне на квадратичен член (за нелинейност),
      - Облачност: експоненциално намаляване с параметър k = 1.0.
    
    Връща:
      float: Крайният коригиращ коефициент.
    """
    temp_coeff = -0.0044
    temp_diff = temp_c - 25  # отклонение от стандартната температура 25°C
    f_temp = 1 + temp_coeff * temp_diff + 0.0001 * (temp_diff ** 2)
    k = 1.0
    f_cloud = math.exp(-k * cloud_cover)
    return f_temp * f_cloud

def calculate_system_production(
    panel_power: float, 
    temp_c: float, 
    cloud_cover: float, 
    num_panels: int, 
    forecast_date: datetime, 
    commissioning_date: datetime, 
    degradation_rate: float = 0.0,
    string_loss_factor: float = 0.98,
    inverter_efficiency: float = 0.95
) -> float:
    """
    Изчислява прогнозирана обща мощност на системата (W), като отчита:
      - базова мощност на панела (panel_power),
      - коригиращ коефициент (влияние на температура и облачност),
      - брой панели,
      - загуби в струнни връзки и инвертор.
    
    Параметри:
      panel_power (float): Изчислена мощност на един панел (W) от PVLib.
      temp_c (float): Температура на въздуха в °C.
      cloud_cover (float): Облачност (в дялове от 0 до 1).
      num_panels (int): Общо брой панели.
      forecast_date (datetime): Дата на прогноза.
      commissioning_date (datetime): Дата на въвеждане в експлоатация.
      degradation_rate (float): Годишна деградация (в проценти), по подразбиране 0 (без деградация).
      string_loss_factor (float): Коефициент на ефективност на струнните връзки (например 0.98).
      inverter_efficiency (float): Ефективност на инвертора (например 0.95).
    
    Логика:
      1. Изчислява корекция чрез production_correction.
      2. Крайната мощност без загуби = panel_power * корекция * num_panels.
      3. След това крайната мощност се коригира с коефициенти загуби:
            production = production_without_losses * string_loss_factor * inverter_efficiency.
    
    Връща:
      float: Крайна мощност на системата (W).
    """
    correction = production_correction(temp_c, cloud_cover)
    production_without_losses = panel_power * correction * num_panels
    
    # Ако се налага да се отчита деградация, може да се добави изчисление тук (напр. по години),
    # но в този вариант не я отчитаме (degradation_rate по подразбиране е 0).
    # production_without_losses *= degradation_factor   (ако е необходимо)
    
    production = production_without_losses * string_loss_factor * inverter_efficiency
    return production
