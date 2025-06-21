from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv
from pathlib import Path

# Chargement des variables d'environnement
API_KEY = os.getenv('METALS_API_KEY')

SYMBOL = "EUR"
BASE = "XAU"
TABLE_ID = "feisty-coder-461708-m9.data_bronze.RAW_GOLD_OHCL"

# Configuration du DAG
default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(seconds=10)
}

def fetch_and_push(**context):
    today = datetime.today().strftime("%Y-%m-%d")

    url = f"https://metals-api.com/api/open-high-low-close/{today}"
    params = {
        "access_key": API_KEY,
        "base": BASE,
        "symbols": SYMBOL
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        result = response.json()
        if result.get("success", False):
            rates = result.get("rates", {})
            data = {
                "date": today,
                "open": rates.get("open"),
                "high": rates.get("high"),
                "low": rates.get("low"),
                "close": rates.get("close")
            }

            client = bigquery.Client()
            errors = client.insert_rows_json(TABLE_ID, [data])

            if errors:
                raise RuntimeError(f"Échec d'insertion dans BigQuery : {errors}")
            else:
                print("✅ Donnée insérée avec succès :",data)

        else:
            print(f"Erreur pour la date {today}: {result.get('error', {}).get('info')}")
    else:
        print(f"Requête échouée pour la date {today}")
        print(response.status_code, response.text)

# Définition du DAG via un bloc `with`
with DAG(
    dag_id="daily_gold_price_to_bigquery",
    default_args=default_args,
    description="Récupère le prix spot de l'or chaque jour et l'insère dans BigQuery",
    schedule_interval="0 20 * * *",  # Tous les jours à 20h UTC
    start_date=datetime(2025, 6, 7),
    catchup=False,
    tags=["gold", "bigquery"]
) as dag:

    fetch_gold = PythonOperator(
        task_id="fetch_gold_price",
        python_callable=fetch_and_push,
    )
