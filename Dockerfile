# Single app image for oms, pms, risk, market_data, and scheduler (compose overrides CMD per service).
# Build once: "docker compose build oms" — other Python services reuse the same image.
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY oms/ oms/
COPY pms/ pms/
COPY risk/ risk/
COPY market_data/ market_data/
COPY scheduler/ scheduler/
COPY scripts/ scripts/
COPY alembic/ alembic/
COPY alembic.ini .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default: run OMS main loop (override in docker-compose for other services)
CMD ["python", "-m", "oms.main"]
