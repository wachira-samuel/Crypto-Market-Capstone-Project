# Crypto-Market-Capstone-Project

## About This Project

This repository showcases my implementation of a production-style distributed data
pipeline that ingests, processes, and stores real-time cryptocurrency market data
using modern data engineering tools. This project emphasizes my capabilities in
using industry-standard technologies; Binance and Kraken APIs, PostgreSQL, Apache
Kafka and Cassandra orchestrated end-to-end with Apache Airflow to handle
real-world, high-velocity data workflows effectively.

## Project Architecture

<img width="1536" height="1024" alt="May" src="https://github.com/user-attachments/assets/bdc35f3d-2cfc-4c16-a520-8fe555785cd4" />


| Layer | Technology | Role |
|---|---|---|
| Ingestion | Airflow + Binance/Kraken API | Fetch, validate & store raw data |
| Staging DB | PostgreSQL | Raw data buffer with structured schema |
| Streaming | Apache Kafka | Real-time data movement across pipeline |
| Serving DB | Cassandra | Fast-access store for analytics & dashboards |
| Orchestration | Apache Airflow | DAG-based scheduling, retries & monitoring |

## Skills Demonstrated
- **API Interaction** - Automated data extraction from Binance and Karen API's
- **Stream Processing** - Real-time data pipelines with Apache Kafka.
- **Distributed storage** - Structured staging in PostgreSQL and fast access serving in cassandra
- **Pipeline Orchestration** - DAG-based scheduling retries amd monitoring with Apache Airflow.
- **Data Engineering Practicess** - Schema validaion, secret management and reproducible infrastructure.

## Technologies Used
- Python 3.11
- Apache Airflow
- Apache Kafka + Schema Registry
- PostgreSQL
- Apache Cassandra
- Binance API / Kraken API

## Data Sources
### 1. Binance API *(Primary — Required)*
- Live price ticks
- Trading pairs
- Order book data
- Historical OHLCV data

### 2. Kraken API *(Recommended)*
- Real-time trade data
- OHLC candlestick data
- Asset pairs
- Market depth

## Pipeline Layer Specifications
### Layer 1 - Data Ingestion
- Fetch live and historical data from Binance or Kraken API's
- Validate incoming payloads (Schema checks, null handling)
- Store raw data in PostgreSQL for staging.

### Layer 2 - Staging Database (PostgreSQL)
Maintains the following structured tables:
- `trades` - individual executed trades.
- `prices` - tick-level price records.
- `symbols` - supported trading price records.
- `timestamps` - ingestion audit trail.

### Layer 3 - Streaming Layer (Apache Kafka)
- Stream data from PostgreSQL to Kafka topics.
- Schema registry for data validation and schema evolution.
- Kafka UI /Control center fro monitoring topic lag and throughput.

### Layer 4 — Serving Layer (Apache Cassandra)
- Optimised for time-series writes at high throughput
- Supports fast lookups for dashboards and alerting systems
- Schema designed around query patterns, not normalised relationships

## Orchestration with Apache Airflow

Apache Airflow is the orchestration backbone of the entire pipeline. Every stage is managed as a task inside a DAG that enforces dependencies, retries on failure and provides full observability through the Airflow UI.

### DAG Flow

<img width="1440" height="974" alt="image" src="https://github.com/user-attachments/assets/91446581-1114-4ea0-afb7-c3381373dc7c" />

### Airflow Task Reference

| Task ID | Operator | Responsibility |
|---|---|---|
| `fetch_binance_data` | PythonOperator | Calls Binance/Kraken API, stores raw JSON in PostgreSQL |
| `validate_raw_data` | PythonOperator | Checks schema, nulls, and data freshness |
| `stream_to_kafka` | PythonOperator | Reads PostgreSQL rows, publishes to Kafka topics |
| `verify_cassandra_load` | PythonOperator | Spot-checks row counts in Cassandra after Spark writes |

### DAG Definition — `dags/crypto_market_etl.py`

    from airflow import DAG
    fromairflow.operators.python import PythonOperator
    from datetime import datetime
    import subprocess

  
    def fetch_data():
    subprocess.run(["python","/opt/airflow/scripts/fetch_crypto_data.py"])
    
    def stream_kafka():
    subprocess.run(["python", "/opt/airflow/scripts/kakfka_producer.py"])
    
    with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2026,5,22),
    schedule= "@hourly",
    catchup = False
    ) as dag:
    
    task1 =PythonOperator(
        task_id ="fetch_crypto_data",
        python_callable =fetch_data
    )
    
    task2= PythonOperator(
        task_id ="stream_to_kafka",
        python_callable = stream_kafka
    )
    
    task1 >> task2


### Airflow Setup

**Install Airflow with required providers:**

```
pip install apache-airflow apache-airflow-providers-apache-kafka apache-airflow-providers-apache-kafka apache-airflow-providers postgres
```
**Initialoze the database:**
```
airflow db init
airflow users create --username admin --password admin \
    --role Admin --firstname Admin --lastname User --email admin@example.com
```

**Set Airflow Variables (Admin → Variables or CLI):**
```
airflow variables set BINANCE_API_KEY    "your_key_here"
airflow variables set BINANCE_SECRET_KEY "your_secret_here"
airflow variables set POSTGRES_DSN       "postgresql://admin:strongpassword@localhost/crypto_pipeline"
airflow variables set KAFKA_BROKERS      "localhost:9092"
airflow variables set CASSANDRA_HOST     "localhost"
```

**Start Airflow:**
```
cp dags/crypto_market_etl.py ~/airflow/dags/
airflow scheduler 
airflow webserver --port 8080
```

## Environment Variables & Security

All secrets must be stored in a `.env` file and never committed to source control.

```
# Binance
BINANCE_API_KEY=your_key_here
BINANCE_SECRET_KEY=your_secret_here

# Kraken (recommended)
KRAKEN_API_KEY=your_key_here
KRAKEN_SECRET_KEY=your_secret_here

# PostgreSQL
POSTGRES_USER=admin
POSTGRES_PASSWORD=strongpassword
POSTGRES_DB=crypto_pipeline

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Cassandra
CASSANDRA_HOST=localhost
CASSANDRA_KEYSPACE=crypto_analytics
```
## Project FIle Structure


                ┌──────────────────────┐
                │ Binance / Kraken API │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │      Airflow         │
                │  Ingestion & ETL     │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │     PostgreSQL       │
                │   Raw/Staging Data   │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │        Kafka         │
                │   Streaming Layer    │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │      Cassandra       │
                │ Processed Analytics  │
                └──────────────────────┘


```
crypto_pipeline/
│
├── dags/
│   └── crypto_pipeline_dag.py
│
├── scripts/
│   ├── fetch_crypto_data.py
│   ├── kafka_producer.py
│   ├── spark_stream.py
│   └── cassandra_loader.py
│
├── requirements.txt
├── docker-compose.yml
├── .env
└── .gitignore
```

## Run commands

## Start Docker containers
    docker-compose up -d

## Run Airflow
    airflow standalone

## Run Spark stream
    spark-submit scripts/spark_stream.py

## Run Cassandra loader
    python scripts/cassandra_loader.py



## Contributions
Contributions are welcome! Kindly fork this repository and submit a pull request with your proposed changes.

## License
This project is licensed under the Mozilla Public License 2.0 — see the [LICENSE](LICENSE) file for details.


