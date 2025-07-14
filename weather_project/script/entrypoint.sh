#!/bin/bash
set -e

# Install dependencies (avoid --user inside venv/Docker)
if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install -r /opt/airflow/requirements.txt
fi

# Run standalone to:
# 1. Init DB
# 2. Create admin user
# 3. Start scheduler + webserver
exec airflow standalone
