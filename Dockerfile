# Multistrat app image: OMS and other Python services
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY oms/ oms/
COPY pms/ pms/
COPY alembic/ alembic/
COPY alembic.ini .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default: run OMS main loop (override in docker-compose for other services)
CMD ["python", "-m", "oms.main"]
