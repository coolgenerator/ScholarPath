FROM python:3.12-slim

WORKDIR /app

# System deps for asyncpg and pgvector
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY pyproject.toml .
COPY scholarpath/__init__.py scholarpath/__init__.py
RUN pip install --no-cache-dir -e .

# Copy application code
COPY alembic.ini .
COPY alembic/ alembic/
COPY scholarpath/ scholarpath/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
CMD ["uvicorn", "scholarpath.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
