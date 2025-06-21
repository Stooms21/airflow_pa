from google.cloud import bigquery
import requests
from datetime import datetime
from fredapi import Fred
import os

# FRED client (clé lue depuis variable d'environnement)
FRED_API_KEY = os.getenv("FRED_API_KEY")
fred = Fred(api_key=FRED_API_KEY)

# Table cible unique
BQ_TABLE = "feisty-coder-461708-m9.market_predictions_data.assets_prices"


def insert_data_to_bigquery(rows):
    client = bigquery.Client()
    errors = client.insert_rows_json(BQ_TABLE, rows)
    if errors:
        raise RuntimeError(f"Erreur d'insertion BQ : {errors}")


def fetch_metals_api(base, symbol, asset_id):
    API_KEY = os.getenv("METALS_API_KEY")
    today = datetime.today().strftime("%Y-%m-%d")
    url = f"https://metals-api.com/api/open-high-low-close/{today}"
    params = {
        "access_key": API_KEY,
        "base": base,
        "symbols": symbol
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        result = response.json()
        if result.get("success", False):
            rates = result.get("rates", {})
            return [{
                "date": today,
                "asset_id": asset_id,
                "source": "metals-api",
                "open": rates.get("open"),
                "high": rates.get("high"),
                "low": rates.get("low"),
                "close": rates.get("close"),
                "volume": None
            }]
    return []


def fetch_fred_series(series_code, asset_id):
    data = fred.get_series(series_code)
    today = datetime.today().date()
    if today in data.index:
        value = data[today]
        return [{
            "date": today.strftime("%Y-%m-%d"),
            "asset_id": asset_id,
            "source": "fred",
            "open": None,
            "high": None,
            "low": None,
            "close": float(value),
            "volume": None
        }]
    return []
