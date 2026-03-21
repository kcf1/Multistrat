# Single app image for oms, pms, risk, and market_data (docker-compose overrides CMD per service).
# Build once: "docker compose build oms" — pms and risk use the same image.
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
COPY alembic/ alembic/
COPY alembic.ini .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default: run OMS main loop (override in docker-compose for other services)
CMD ["python", "-m", "oms.main"]
