FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CONFIG_PATH=/app/config.yaml

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY monitor/requirements.txt /app/monitor/requirements.txt
RUN pip install --no-cache-dir -r /app/monitor/requirements.txt

COPY collector /app/collector
COPY monitor /app/monitor
COPY config.yaml /app/config.yaml

EXPOSE 8133

CMD ["uvicorn", "monitor.app:app", "--host", "0.0.0.0", "--port", "8133"]
