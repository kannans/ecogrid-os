# EcoGrid OS — Phase 1 ingestion worker image
# Slim, non-root, no build toolchain in the final layer.

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    ECOGRID_SPOOL_PATH=/app/data/spool/telemetry-spool.jsonl \
    ECOGRID_HEARTBEAT_PATH=/app/data/heartbeat

WORKDIR /app

# Runtime deps only (curl for ad-hoc container debugging).
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Dependency layer first so code edits do not invalidate the pip cache.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_grid.py ./

# Non-root runtime user; owns the spool/heartbeat volume.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin ecogrid \
 && mkdir -p /app/data/spool \
 && chown -R ecogrid:ecogrid /app
USER ecogrid

# Liveness = "did the last cycle complete?" The worker touches a heartbeat file
# after every successful cycle; 3 missed cycles (900s) marks the pod unhealthy.
HEALTHCHECK --interval=60s --timeout=10s --start-period=45s --retries=3 \
  CMD python -c "import os,sys,time; p=os.environ.get('ECOGRID_HEARTBEAT_PATH','/app/data/heartbeat'); sys.exit(0 if os.path.exists(p) and time.time()-os.path.getmtime(p) < 900 else 1)"

ENTRYPOINT ["python", "-u", "ingest_grid.py"]
CMD []
