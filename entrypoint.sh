#!/bin/bash
set -e

echo "==> Initializing database (create tables + pgvector extension)..."
python -m scholarpath.init_db

echo "==> Starting: $@"
exec "$@"
