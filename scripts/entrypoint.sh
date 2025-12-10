#!/bin/bash
set -e

# Upgrade pip and install Python packages from requirements.txt
python3 -m pip install --upgrade pip
pip install -r /opt/airflow/requirements.txt

# Create an Airflow admin user if it doesn't already exist
if ! airflow users list | grep -q "admin"; then
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade Airflow metadata database to the latest version
airflow db upgrade

# Starts airflow webserver
exec airflow webserver