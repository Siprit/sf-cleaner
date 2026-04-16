FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir hatchling

COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]"

COPY . .

ENV PYTHONUNBUFFERED=1
