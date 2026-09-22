FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml ./
RUN uv venv && uv pip install pyspark==3.5.1 pyarrow>=15.0.0 click>=8.1.7 pyyaml>=6.0

COPY src/ ./src/
COPY configs/ ./configs/

ENTRYPOINT [ "uv", "run", "python", "-m", "src.ingestion.ingest_batch" ]