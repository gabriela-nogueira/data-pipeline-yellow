#!/bin/bash

# Inicializa o banco de dados do Airflow
airflow db init

# Cria um usuário admin default
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname Airflow \
    --role Admin \
    --email gabriielanogueiira@gmail.com \
    --password airflow

# Inicia o scheduler e o webserver
airflow scheduler &
exec airflow webserver --port 8080
