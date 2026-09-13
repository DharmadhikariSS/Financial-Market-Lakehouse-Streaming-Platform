# ==============================================================================
# MULTI-STAGE CONTAINER BUILD: MILESTONE 02
# Base Image: Python 3.11-slim
# Security: Non-root user execution (appuser:10001)
# ==============================================================================

# STAGE 1: Dependency Builder
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .

# Install dependencies into temporary wheel cache
RUN pip install --no-cache-dir --upgrade pip && \
    pip wheel --no-cache-dir --wheel-dir /build/wheels -e .

# STAGE 2: Distroless / Slim Production Runtime
FROM python:3.11-slim AS runtime

WORKDIR /app

# Security: Create non-root unprivileged service account
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /bin/bash -m appuser

# Copy and install pre-built wheels
COPY --from=builder /build/wheels /wheels
RUN pip install --no-cache-dir /wheels/* && rm -rf /wheels

# Copy application source code and configuration
COPY pyproject.toml .
COPY src/ src/
COPY data/ data/

# Set ownership to unprivileged user
RUN chown -R appuser:appgroup /app

USER appuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "from src.common.config import settings; print('Config OK')" || exit 1

ENTRYPOINT ["python", "-m", "src.02_containerized_scheduled_batch.runner"]
