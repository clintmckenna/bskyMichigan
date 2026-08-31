FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CONFIG_PATH=/app/config.yaml

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY collector/requirements.txt /app/collector/requirements.txt
RUN pip install --no-cache-dir -r /app/collector/requirements.txt

# Copy application source
COPY collector /app/collector
COPY config.yaml /app/config.yaml

# Ensure data directory exists
RUN mkdir -p /data/raw_snapshots /data/logs

# Default command
CMD ["python", "-m", "collector.firehose_listener"]
