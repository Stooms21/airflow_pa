from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv


# Paramètres d'environnement (à définir dans Airflow ou .env)
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TABLE_SRC = os.getenv("CRYPTO_KLINES_1M_TABLE_ID")
TABLE_DEST = os.getenv("CRYPTO_KLINES_1H_TABLE_ID")

if not PROJECT_ID or not TABLE_SRC or not TABLE_DEST:
    raise ValueError("Veuillez définir les variables d’environnement : GCP_PROJECT_ID, CRYPTO_KLINES_1M_TABLE_ID, CRYPTO_KLINES_1H_TABLE_ID")

# Requête SQL MERGE (1h)
MERGE_KLINES_1H_QUERY = f"""
MERGE INTO `{PROJECT_ID}.{TABLE_DEST}` T
USING (
  SELECT
    TIMESTAMP_TRUNC(timestamp_utc, HOUR) AS timestamp_hour,
    symbol,
    ANY_VALUE(open_first) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    ANY_VALUE(close_last) AS close,
    SUM(volume) AS volume,
    SUM(quote_volume) AS quote_volume,
    SUM(nb_trades) AS nb_trades
  FROM (
    SELECT
      timestamp_utc,
      symbol,
      FIRST_VALUE(open) OVER w AS open_first,
      LAST_VALUE(close) OVER w AS close_last,
      high,
      low,
      volume,
      quote_volume,
      nb_trades
    FROM `{PROJECT_ID}.{TABLE_SRC}`
    WHERE timestamp_utc >= TIMESTAMP_TRUNC(TIMESTAMP('{{ execution_date }}'), HOUR)
      AND timestamp_utc < TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP('{{ execution_date }}'), HOUR), INTERVAL 1 HOUR)
    WINDOW w AS (
      PARTITION BY symbol
      ORDER BY timestamp_utc
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
  )
  GROUP BY timestamp_hour, symbol
) AS S
ON T.timestamp_utc = S.timestamp_hour AND T.symbol = S.symbol
WHEN MATCHED THEN UPDATE SET
  open = S.open,
  high = S.high,
  low = S.low,
  close = S.close,
  volume = S.volume,
  quote_volume = S.quote_volume,
  nb_trades = S.nb_trades
WHEN NOT MATCHED THEN INSERT (
  timestamp_utc, symbol, open, high, low, close, volume, quote_volume, nb_trades
) VALUES (
  S.timestamp_hour, S.symbol, S.open, S.high, S.low, S.close, S.volume, S.quote_volume, S.nb_trades
)
"""


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="aggregate_klines_1h_merge_env_v2",
    description="Agrège les données 1 minute en 1 heure et met à jour la table BigQuery",
    start_date=datetime(2025, 6, 16),
    schedule_interval="0 * * * *",  # Every hour at 00 minutes
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["binance", "klines", "bq", "agg"],
    doc_md="""
### DAG : Agrégation des données Binance 1m → 1h
- Source : table `RAW_CRYPTO_KLINES_1MIN`
- Destination : table `RAW_CRYPTO_KLINES_1H`
- Opération : MERGE BigQuery horaire
"""
) as dag:

    merge_hourly_klines = BigQueryInsertJobOperator(
        task_id="merge_klines_hourly",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": MERGE_KLINES_1H_QUERY,
                "useLegacySql": False
            }
        }
    )

    end = DummyOperator(task_id="end")

    merge_hourly_klines >> end
