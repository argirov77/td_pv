# Използваме лек базов образ с Python 3.10
FROM python:3.10-slim

# Задаваме работната директория в контейнера
WORKDIR /app

# Копираме файла със зависимости
COPY requirements.txt .

# Инсталираме системни библиотеки за ML зависимости (напр. LightGBM изисква libgomp)
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Актуализираме pip и инсталираме зависимостите без кеш
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Копираме всички изходни файлове на приложението
COPY . .

# По избор: създаване на непотребителски акаунт, за да не се работи като root
# RUN useradd -m appuser
# USER appuser

# Отваряме порт 8000 за FastAPI
EXPOSE 8000

# Стандартна команда за стартиране на FastAPI чрез Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
