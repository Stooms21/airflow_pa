from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from pandas_gbq import to_gbq
import os
from dotenv import load_dotenv


PROJECT_ID = os.getenv("GCP_PROJECT_ID")
KLINES_TABLE_ID = os.getenv("CRYPTO_KLINES_1M_TABLE_ID")
TICKERS_TABLE_ID = os.getenv("CRYPTO_TICKERS_1M_TABLE_ID")
SYMBOLS = os.getenv("CRYPTO_SYMBOLS", "BTCUSDT,ETHUSDT").split(",")


# === Fonction principale ===

def fetch_and_store_all():
    timestamp_utc = datetime.utcnow().replace(second=0, microsecond=0)
    fetch_klines(timestamp_utc)
    fetch_tickers(timestamp_utc)


def fetch_klines(timestamp_utc):
    end_time = timestamp_utc
    start_time = end_time - timedelta(minutes=1)
    all_data = []

    for symbol in SYMBOLS:
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": 1
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"⚠️ Erreur API Binance (klines) pour {symbol} : {e}")
            continue

        if not data:
            print(f"⚠️ Aucune donnée kline pour {symbol}")
            continue

        kline = data[0]
        all_data.append({
            "timestamp_utc": timestamp_utc,
            "symbol": symbol,
            "open": float(kline[1]),
            "high": float(kline[2]),
            "low": float(kline[3]),
            "close": float(kline[4]),
            "volume": float(kline[5]),
            "quote_volume": float(kline[7]),
            "nb_trades": int(kline[8])
        })

    if all_data:
        df = pd.DataFrame(all_data)
        print(f"✅ Insertion de {len(df)} klines dans BigQuery")
        to_gbq(
            dataframe=df,
            destination_table=KLINES_TABLE_ID,
            project_id=PROJECT_ID,
            if_exists="append"
        )
    else:
        print("⚠️ Aucune kline à insérer")


def fetch_tickers(timestamp_utc):
    all_data = []

    for symbol in SYMBOLS:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        params = {"symbol": symbol}

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"⚠️ Erreur API Binance (ticker) pour {symbol} : {e}")
            continue

        all_data.append({
            "timestamp_utc": timestamp_utc,
            "symbol": symbol,
            "last_price": float(data["lastPrice"]),
            "price_change": float(data["priceChange"]),
            "price_change_pct": float(data["priceChangePercent"]),
            "bid_price": float(data["bidPrice"]),
            "bid_qty": float(data["bidQty"]),
            "ask_price": float(data["askPrice"]),
            "ask_qty": float(data["askQty"]),
            "volume": float(data["volume"]),
            "quote_volume": float(data["quoteVolume"]),
            "nb_trades": int(data["count"])
        })

    if all_data:
        df = pd.DataFrame(all_data)
        print(f"✅ Insertion de {len(df)} tickers dans BigQuery")
        to_gbq(
            dataframe=df,
            destination_table=TICKERS_TABLE_ID,
            project_id=PROJECT_ID,
            if_exists="append"
        )
    else:
        print("⚠️ Aucune donnée ticker à insérer")


# === Définition du DAG ===

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

with DAG(
    dag_id="crypto_klines_and_tickers_minutely",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    start_date=datetime(2025, 6, 16),
    catchup=False,
    max_active_runs=1,
    tags=["binance", "crypto", "klines", "tickers"]
) as dag:

    fetch_all_task = PythonOperator(
        task_id="fetch_and_store_all",
        python_callable=fetch_and_store_all
    )
