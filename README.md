# Weather ETL Pipeline — Apache Airflow + Docker + SQLite

Pipeline ETL che scarica dati meteo orari di Napoli, li trasforma
e li carica in SQLite, orchestrata da Apache Airflow su Docker.

## Cosa fa
- Scarica dati meteo ogni giorno alle 8:00 da Open-Meteo API
- Salva il JSON grezzo nella cartella data/raw
- Trasforma i dati in record strutturati
- Carica 24 record orari nel database SQLite

## Tecnologie usate
- Apache Airflow 2.9.1
- Docker + Docker Compose
- Python 3
- SQLite
- Open-Meteo API (gratuita, senza API key)

## Struttura del progetto
weather-etl/
├── dags/
│   └── weather_etl.py       # DAG principale
├── scripts/
│   └── check_db.py          # script per vedere i dati
├── docker-compose.yml        # configurazione Docker
├── .gitignore
└── README.md

## Come avviarlo

### 1. Prerequisiti
- Docker Desktop installato e avviato
- VS Code

### 2. Crea il file .env nella cartella principale
AIRFLOW_UID=50000
WEATHER_CITY=Naples
WEATHER_LAT=40.8518
WEATHER_LON=14.2681

### 3. Crea le cartelle necessarie
mkdir data\raw
mkdir data\db
mkdir logs
mkdir plugins

### 4. Avvia i container
docker compose up -d

### 5. Apri Airflow
Vai su http://localhost:8080

Login: admin / admin

### 6. Attiva il DAG
- Cerca weather_etl nella lista
- Clicca il toggle per attivarlo
- Clicca play per eseguirlo subito

### 7. Verifica i dati
docker compose exec airflow-scheduler python -c "import sqlite3; conn = sqlite3.connect('/opt/airflow/data/db/weather.db'); print(conn.execute('SELECT COUNT(*) FROM weather_hourly').fetchone())"

## Pipeline ETL
Open-Meteo API → Extract → Transform → Load → SQLite

I 3 task in sequenza:
1. **extract_weather** — chiama l'API e salva il JSON grezzo
2. **transform_weather** — normalizza i dati in record strutturati  
3. **load_to_sqlite** — inserisce i record nel database

## Dati raccolti
| Colonna | Tipo | Descrizione |
|---|---|---|
| city | TEXT | Nome della città |
| timestamp | TEXT | Data e ora del record |
| temperature_c | REAL | Temperatura in °C |
| humidity_pct | INTEGER | Umidità in % |
| precipitation_mm | REAL | Precipitazioni in mm |
| wind_kmh | REAL | Vento in km/h |

## Autore
Progetto didattico per imparare Apache Airflow con Docker
