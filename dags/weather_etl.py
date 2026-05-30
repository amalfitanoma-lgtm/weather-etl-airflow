import os
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# logger per scrivere nei log di Airflow
log = logging.getLogger(__name__)

# percorsi delle cartelle dentro il container
DATA_DIR = Path("/opt/airflow/data")
RAW_DIR  = DATA_DIR / "raw"
DB_PATH  = DATA_DIR / "db" / "weather.db"

# legge le variabili dal file .env
CITY = os.getenv("WEATHER_CITY", "Naples")
LAT  = os.getenv("WEATHER_LAT",  "40.8518")
LON  = os.getenv("WEATHER_LON",  "14.2681")

# parametri di default per tutti i task
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def extract_weather(**context):
    # data di esecuzione del DAG es. "2024-01-15"
    execution_date = context["ds"]

    # costruzione URL Open-Meteo senza API key
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}"
        f"&longitude={LON}"
        f"&hourly=temperature_2m,relativehumidity_2m,precipitation,windspeed_10m"
        f"&timezone=Europe%2FRome"
        f"&forecast_days=1"
    )

    # chiamata API
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    raw_data = response.json()

    # aggiunge metadati al JSON
    raw_data["_meta"] = {
        "city": CITY,
        "lat": LAT,
        "lon": LON,
        "extracted_at": datetime.utcnow().isoformat(),
        "execution_date": execution_date,
    }

    # salva il JSON grezzo su disco
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = RAW_DIR / f"weather_{CITY}_{execution_date}.json"
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2, ensure_ascii=False)

    # passa il percorso al task successivo
    context["ti"].xcom_push(key="raw_file_path", value=str(raw_file))
    return str(raw_file)


def transform_weather(**context):
    # legge il percorso dal task precedente
    raw_file_path = context["ti"].xcom_pull(
        task_ids="extract_weather",
        key="raw_file_path"
    )

    with open(raw_file_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    hourly = raw_data["hourly"]
    meta   = raw_data["_meta"]

    # combina le 4 liste orarie in record singoli
    records = []
    for time_str, temp, humidity, precip, wind in zip(
        hourly["time"],
        hourly["temperature_2m"],
        hourly["relativehumidity_2m"],
        hourly["precipitation"],
        hourly["windspeed_10m"]
    ):
        record = {
            "city":             meta["city"],
            "lat":              float(meta["lat"]),
            "lon":              float(meta["lon"]),
            "timestamp":        time_str,
            "temperature_c":    round(float(temp), 2) if temp is not None else None,
            "humidity_pct":     int(humidity) if humidity is not None else None,
            "precipitation_mm": round(float(precip), 2) if precip is not None else None,
            "wind_kmh":         round(float(wind), 2) if wind is not None else None,
            "execution_date":   meta["execution_date"],
            "extracted_at":     meta["extracted_at"],
        }
        records.append(record)

    # salva i record trasformati
    transformed_file = RAW_DIR / f"weather_{CITY}_{meta['execution_date']}_transformed.json"
    with open(transformed_file, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    context["ti"].xcom_push(key="transformed_file_path", value=str(transformed_file))
    return str(transformed_file)


def load_to_sqlite(**context):
    # legge il percorso dal task precedente
    transformed_file_path = context["ti"].xcom_pull(
        task_ids="transform_weather",
        key="transformed_file_path"
    )

    with open(transformed_file_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    # connessione a SQLite
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # crea la tabella se non esiste
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_hourly (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            city             TEXT    NOT NULL,
            lat              REAL,
            lon              REAL,
            timestamp        TEXT    NOT NULL,
            temperature_c    REAL,
            humidity_pct     INTEGER,
            precipitation_mm REAL,
            wind_kmh         REAL,
            execution_date   TEXT,
            extracted_at     TEXT,
            inserted_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(city, timestamp)
        )
    """)

    # inserisce i record saltando i duplicati
    inserted = 0
    skipped  = 0
    for record in records:
        cursor.execute("""
            INSERT OR IGNORE INTO weather_hourly
                (city, lat, lon, timestamp, temperature_c,
                 humidity_pct, precipitation_mm, wind_kmh,
                 execution_date, extracted_at)
            VALUES
                (:city, :lat, :lon, :timestamp, :temperature_c,
                 :humidity_pct, :precipitation_mm, :wind_kmh,
                 :execution_date, :extracted_at)
        """, record)
        if cursor.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    conn.close()

    log.info(f"Inseriti: {inserted}, ignorati: {skipped}")
    return {"inserted": inserted, "skipped": skipped}


# definizione del DAG
with DAG(
    dag_id="weather_etl",
    description="Pipeline ETL: API meteo → JSON → SQLite",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 8 * * *",
    catchup=False,
    tags=["weather", "etl", "sqlite"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    load_task = PythonOperator(
        task_id="load_to_sqlite",
        python_callable=load_to_sqlite,
    )

    # ordine di esecuzione
    extract_task >> transform_task >> load_task