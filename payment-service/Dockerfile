# syntax=docker/dockerfile:1

# ---- Stage 1: build. Dependencies are installed here so that pip's cache and any
# build tooling never reach the final image.
FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- Stage 2: runtime. Small base image, non-root user, nothing but the app.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN groupadd --system app \
 && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app \
 && mkdir /data && chown app:app /data
WORKDIR /app
COPY --from=builder /install /usr/local
COPY app ./app
USER app
EXPOSE 8000
# Readiness-based health check: healthy only once the broker consumer is bound.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=6 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=2)"
# Configuration (BROKER_URL, DB_PATH, LOG_LEVEL) comes from environment variables.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
