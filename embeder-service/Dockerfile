FROM python:3.11-slim

WORKDIR /app

# Szybsze instalacje
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install -r requirements.txt

# W prod skopiujemy kod podczas buildu; w dev użyjemy bind mountu
COPY . .

# Domyślne uruchomienie (możesz nadpisać w compose)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]