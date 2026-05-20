# F1 Data Warehouse

A data warehouse for Formula 1 race data built on a Bronze-Silver-Gold medallion architecture. The pipeline integrates historical data from a CSV data lake with real-time data scraped from the Ergast REST API, orchestrated with Apache Airflow and containerized with Docker.

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────┐     ┌─────────────────┐
│   BRONZE    │────▶│              SILVER               │────▶│      GOLD       │
│             │     │                                   │     │                 │
│ f1_data.csv │     │  MinIO S3 (Parquet, partitioned)  │     │  PostgreSQL     │
│ Ergast API  │     │  Dimensions + Facts               │     │  Constellation  │
└─────────────┘     └──────────────────────────────────┘     │  Schema         │
                                                              └─────────────────┘
```

**Bronze** — raw sources: historical CSV file (2012–2025) and the Ergast REST API.

**Silver** — cleaned and typed data stored as Parquet files in MinIO S3. Fact tables are partitioned by year (`year={year}/data.parquet`). Surrogate keys are assigned at this layer.

**Gold** — final constellation schema loaded into PostgreSQL with proper data types, foreign keys resolved, and data ready for reporting.

## Data Model

### Dimensions
| Table | Description |
|---|---|
| `dimensions.driver` | Driver profiles |
| `dimensions.constructor` | Constructor (team) profiles |
| `dimensions.circuit` | Circuit metadata |
| `dimensions.race` | Race schedule and session dates |
| `dimensions.time` | Date dimension |

### Facts
| Table | Description |
|---|---|
| `facts.race_results` | Race finishing results per driver |
| `facts.laps` | Lap-by-lap times per driver |
| `facts.pitstops` | Pit stop times and durations |
| `facts.driver_standings` | Championship standings per driver after each race |
| `facts.constructor_standings` | Championship standings per constructor after each race |

## Pipeline Overview

### Initial Load (master pipeline)

The `master_f1_etl_pipeline` DAG orchestrates the full historical load:

```
csv_bronze_to_silver
        │
        ▼
api_constructors ─┐
api_drivers      ─┤ (parallel)
api_circuits     ─┤
api_races        ─┘
        │
        ▼
api_results      ─┐
api_laps         ─┤ (parallel)
api_pitstops     ─┤
api_driverstandings ─┤
api_constructorstandings ─┘
        │
        ▼
all_silver_to_gold
```

### Incremental Load (weekly pipeline)

The `weekly_f1_pipeline` DAG runs every Monday at 08:00 UTC. It handles 2026+ data incrementally using Apache Kafka.

**Producer phase** — scrapes new records from the Ergast API, identifies records not yet in Silver, assigns surrogate keys continuing from the existing max, merges into Silver Parquet files, and publishes only new records to Kafka topics.

**Consumer phase** — reads messages from Kafka topics and inserts them into PostgreSQL Gold without dropping or recreating any tables. Kafka offsets are committed only after a successful insert.

Kafka topics:
```
f1.dim.drivers
f1.dim.constructors
f1.dim.circuits
f1.dim.races
f1.fact.results
f1.fact.laps
f1.fact.pitstops
f1.fact.driver_standings
f1.fact.constructor_standings
```

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Streaming | Apache Kafka |
| Object Storage | MinIO S3 |
| Data Warehouse | PostgreSQL |
| File Format | Parquet (via PyArrow) |
| Transformation | Python, Pandas |
| Containerization | Docker |
| Data Source | Ergast F1 API, CSV data lake |

## Project Structure

```
etl/
├── dags/
│   ├── scraper/                         # Ergast API scrapers per entity
│   ├── utils/
│   │   ├── s3_helper.py                 # MinIO read/write helpers
│   │   └── kafka_helper.py              # Kafka producer/consumer helpers
│   ├── csv_bronze_to_silver.py          # Historical CSV -> Silver
│   ├── api_*_bronze_to_silver.py        # API entity -> Silver (one per entity)
│   ├── all_silver_to_gold.py            # Silver -> Gold PostgreSQL
│   ├── master_pipeline.py               # Full historical load orchestrator
│   ├── kafka_producer_pipeline.py       # Weekly incremental producer
│   ├── kafka_consumer_pipeline.py       # Kafka -> Gold consumer
│   └── weekly_f1_pipeline.py            # Unified weekly DAG
├── data/
│   └── bronze/                          # Raw CSV source file
└── docker-compose.yaml
```

## Running the Project

**Requirements:** Docker and Docker Compose.

```bash
# Start all services (Airflow, Kafka, MinIO, PostgreSQL)
docker compose up -d
```

Airflow UI is available at `http://localhost:8080`.

**Initial load** — trigger `master_f1_etl_pipeline` manually from the Airflow UI. This runs the full Bronze-Silver-Gold pipeline for all historical data.

**Incremental load** — `weekly_f1_pipeline` runs automatically every Monday at 08:00 UTC and picks up any new 2026+ races, results, laps, and standings.
