#!/bin/bash
set -e

# Instala dependencias personalizadas
pip install -r /opt/airflow/requirements.txt

# Inicializa base de datos de Airflow (solo la primera vez)
airflow db init

# Crea usuario admin si no existe
if ! airflow users list | grep -q "admin@example.com"; then
  echo "Creating admin user..."
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
else
  echo "Admin user already exists."
fi

# Inicia los servicios
airflow webserver -p 8080 &
airflow scheduler
