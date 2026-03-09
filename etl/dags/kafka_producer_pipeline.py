# dags/kafka_producer_pipeline.py

"""
F1 Kafka Producer Pipeline: Ergast API (2026+) -> Silver -> Kafka

Purpose:
    Incremental weekly loading of 2026+ F1 data.
    Does NOT touch data already loaded by master_pipeline (CSV 2012-2023, API 2024-2025).

Schedule:
    Every Monday at 08:00 UTC.

Architecture:
    1. Scrape 2026+ data from Ergast API (reuses existing scraper classes)
    2. Identify NEW records (not yet in Silver MinIO)
    3. Assign surrogate keys (continuing from max existing key)
    4. Merge into Silver Parquet files (idempotent)
    5. Publish ONLY new records to Kafka topics
    6. Trigger consumer DAG

Kafka Topics:
    Dimensions : f1.dim.drivers, f1.dim.constructors, f1.dim.circuits, f1.dim.races
    Facts      : f1.fact.results, f1.fact.laps, f1.fact.pitstops,
                 f1.fact.driver_standings, f1.fact.constructor_standings

Key concept (Kafka):
    Producer sends each record as a JSON message.
    Message key = natural key of the entity (used for partitioning).
    Producer does NOT know or care who will consume the messages.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.s3_helper import MinIOHelper
from utils.kafka_helper import KafkaProducerHelper
from scraper.driver import DriverScraper
from scraper.constructor import ConstructorScraper
from scraper.circuit import CircuitScraper
from scraper.race import RaceScraper
from scraper.result import ResultScraper
from scraper.lap import LapScraper
from scraper.pitstop import PitStopScraper
from scraper.driverStanding import DriverStandingScraper
from scraper.constructorStanding import ConstructorStandingScraper


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

SILVER_BUCKET          = 'f1-silver'
KAFKA_BOOTSTRAP        = 'kafka:29092'
INCREMENTAL_YEAR_START = 2026

# ── Kafka topics ──────────────────────────────────────────────────────────────
TOPIC_DRIVERS               = 'f1.dim.drivers'
TOPIC_CONSTRUCTORS          = 'f1.dim.constructors'
TOPIC_CIRCUITS              = 'f1.dim.circuits'
TOPIC_RACES                 = 'f1.dim.races'
TOPIC_RESULTS               = 'f1.fact.results'
TOPIC_LAPS                  = 'f1.fact.laps'
TOPIC_PITSTOPS              = 'f1.fact.pitstops'
TOPIC_DRIVER_STANDINGS      = 'f1.fact.driver_standings'
TOPIC_CONSTRUCTOR_STANDINGS = 'f1.fact.constructor_standings'


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _current_year() -> int:
    return datetime.now().year


def _publish_to_kafka(df: pd.DataFrame, topic: str, key_field: str, label: str) -> int:
    """Serialize DataFrame rows to JSON and publish each row as a Kafka message."""
    if df.empty:
        print(f"  No new {label} to publish to Kafka")
        return 0
    records = df.to_dict(orient='records')
    kafka = KafkaProducerHelper(bootstrap_servers=KAFKA_BOOTSTRAP, topic=topic)
    kafka.send_all(records, key_field=key_field)
    print(f"  Published {len(records)} new {label} to topic '{topic}'")
    return len(records)


def _load_dimension_lookups(minio: MinIOHelper) -> dict:
    """
    Load Silver dimension files and build FK lookup dicts.
    Called by all fact tasks for FK resolution.
    """
    dim_driver      = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    dim_constructor = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    dim_race        = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    dim_time        = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_time/dim_time.parquet')

    dim_race['_composite'] = dim_race['race_year'].astype(str) + '_' + dim_race['race_round'].astype(str)
    race_to_date = dict(zip(dim_race['race_key'], dim_race['race_date']))
    time_map     = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))

    return {
        'driver_lookup':      dict(zip(dim_driver['driver_ref'],           dim_driver['driver_key'])),
        'constructor_lookup': dict(zip(dim_constructor['constructor_ref'], dim_constructor['constructor_key'])),
        'race_lookup':        dict(zip(dim_race['_composite'],             dim_race['race_key'])),
        'race_to_date':       race_to_date,
        'time_map':           time_map,
        'race_year_map':      dict(zip(dim_race['race_key'],               dim_race['race_year'])),
    }


def _safe_max_id(df: pd.DataFrame, col: str, csv_source: str, fallback: int) -> int:
    """Return max natural ID for API records, or fallback if none exist."""
    api_rows = df[df['source'] != csv_source] if not df.empty else pd.DataFrame()
    if api_rows.empty or api_rows[col].isna().all():
        return fallback
    return int(api_rows[col].max())


# ═══════════════════════════════════════════════════════════════════════════════
# DIMENSION TASKS
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_publish_drivers(**context):
    """
    Scrape 2026+ drivers, find new ones (not in Silver), save to Silver, publish to Kafka.
    New driver = driver_ref not present in existing dim_driver Silver file.
    """
    print("=" * 70)
    print("PRODUCER: dim_driver  (2026+)")
    print("=" * 70)

    minio        = MinIOHelper()
    scraper      = DriverScraper()
    current_year = _current_year()

    # Step 1: Scrape + transform
    df_raw = scraper.scrape_drivers_range(INCREMENTAL_YEAR_START, current_year)
    df_new = scraper.transform_to_silver_schema(df_raw)

    # Step 2: Load existing Silver
    try:
        existing = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    except Exception:
        existing = pd.DataFrame()

    existing_refs = set(existing['driver_ref'].values) if not existing.empty else set()
    max_key = int(existing['driver_key'].max()) if not existing.empty else 0
    max_id  = _safe_max_id(existing, 'driver_id', 'f1_data.csv', 10000)

    # Step 3: Filter to new records
    df_truly_new = df_new[~df_new['driver_ref'].isin(existing_refs)].copy().reset_index(drop=True)

    if df_truly_new.empty:
        print("  No new drivers found this week")
        context['ti'].xcom_push(key='drivers_published', value=0)
        return {'new_count': 0}

    # Step 4: Assign surrogate + natural keys
    df_truly_new['driver_key'] = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['driver_id']  = range(max_id  + 1, max_id  + len(df_truly_new) + 1)
    df_truly_new['source']     = 'api_incremental'
    df_truly_new['created_at'] = datetime.now()

    column_order = [
        'driver_key', 'driver_id', 'driver_ref', 'driver_number', 'driver_code',
        'driver_forename', 'driver_surname', 'driver_dob', 'driver_nationality',
        'driver_url', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    # Step 5: Merge with Silver and save
    merged = pd.concat([existing, df_truly_new], ignore_index=True) if not existing.empty else df_truly_new
    minio.upload_parquet(merged, SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    print(f"  Silver updated: {len(merged)} total drivers")

    # Step 6: Publish new records to Kafka
    count = _publish_to_kafka(df_truly_new, TOPIC_DRIVERS, 'driver_ref', 'drivers')
    context['ti'].xcom_push(key='drivers_published', value=count)
    return {'new_count': count}


def scrape_publish_constructors(**context):
    """
    Scrape 2026+ constructors, find new ones, save to Silver, publish to Kafka.
    New constructor = constructor_ref not present in existing Silver.
    """
    print("=" * 70)
    print("PRODUCER: dim_constructor  (2026+)")
    print("=" * 70)

    minio        = MinIOHelper()
    scraper      = ConstructorScraper()
    current_year = _current_year()

    df_raw = scraper.scrape_constructors_range(INCREMENTAL_YEAR_START, current_year)
    df_new = scraper.transform_to_silver_schema(df_raw)

    try:
        existing = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    except Exception:
        existing = pd.DataFrame()

    existing_refs = set(existing['constructor_ref'].values) if not existing.empty else set()
    max_key = int(existing['constructor_key'].max()) if not existing.empty else 0
    max_id  = _safe_max_id(existing, 'constructor_id', 'f1_data.csv', 10000)

    df_truly_new = df_new[~df_new['constructor_ref'].isin(existing_refs)].copy().reset_index(drop=True)

    if df_truly_new.empty:
        print("  No new constructors found this week")
        context['ti'].xcom_push(key='constructors_published', value=0)
        return {'new_count': 0}

    df_truly_new['constructor_key'] = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['constructor_id']  = range(max_id  + 1, max_id  + len(df_truly_new) + 1)
    df_truly_new['source']          = 'api_incremental'
    df_truly_new['created_at']      = datetime.now()

    column_order = [
        'constructor_key', 'constructor_id', 'constructor_ref', 'constructor_name',
        'constructor_nationality', 'constructor_url', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    merged = pd.concat([existing, df_truly_new], ignore_index=True) if not existing.empty else df_truly_new
    minio.upload_parquet(merged, SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    print(f"  Silver updated: {len(merged)} total constructors")

    count = _publish_to_kafka(df_truly_new, TOPIC_CONSTRUCTORS, 'constructor_ref', 'constructors')
    context['ti'].xcom_push(key='constructors_published', value=count)
    return {'new_count': count}


def scrape_publish_circuits(**context):
    """
    Fetch all-time circuits from API, find new ones (e.g. new 2026 venues), publish to Kafka.
    CircuitScraper fetches all circuits in one call (no year range needed).
    """
    print("=" * 70)
    print("PRODUCER: dim_circuit  (new venues)")
    print("=" * 70)

    minio   = MinIOHelper()
    scraper = CircuitScraper()

    df_raw = scraper.scrape_all_circuits()
    df_new = scraper.transform_to_silver_schema(df_raw)

    try:
        existing = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_circuit/dim_circuit.parquet')
    except Exception:
        existing = pd.DataFrame()

    existing_refs = set(existing['circuit_references'].values) if not existing.empty else set()
    max_key = int(existing['circuit_key'].max()) if not existing.empty else 0
    max_id  = _safe_max_id(existing, 'circuit_id', 'f1_data.csv', 10000)

    df_truly_new = df_new[~df_new['circuit_references'].isin(existing_refs)].copy().reset_index(drop=True)

    if df_truly_new.empty:
        print("  No new circuits found")
        context['ti'].xcom_push(key='circuits_published', value=0)
        return {'new_count': 0}

    df_truly_new['circuit_key'] = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['circuit_id']  = range(max_id  + 1, max_id  + len(df_truly_new) + 1)
    df_truly_new['source']      = 'api_incremental'
    df_truly_new['created_at']  = datetime.now()

    column_order = [
        'circuit_key', 'circuit_id', 'circuit_references', 'circuit_name',
        'circuit_city', 'circuit_country', 'circuit_latitude', 'circuit_longtitude',
        'circuit_altitude', 'circuit_url', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    merged = pd.concat([existing, df_truly_new], ignore_index=True) if not existing.empty else df_truly_new
    minio.upload_parquet(merged, SILVER_BUCKET, 'dimensions/dim_circuit/dim_circuit.parquet')
    print(f"  Silver updated: {len(merged)} total circuits")

    count = _publish_to_kafka(df_truly_new, TOPIC_CIRCUITS, 'circuit_references', 'circuits')
    context['ti'].xcom_push(key='circuits_published', value=count)
    return {'new_count': count}


def scrape_publish_races(**context):
    """
    Scrape 2026+ races, resolve circuit_key FK, find new races, publish to Kafka.
    New race = (race_year + race_round) combination not in existing Silver.
    """
    print("=" * 70)
    print("PRODUCER: dim_race  (2026+)")
    print("=" * 70)

    minio        = MinIOHelper()
    scraper      = RaceScraper()
    current_year = _current_year()

    df_raw = scraper.scrape_races_range(INCREMENTAL_YEAR_START, current_year)
    df_new = scraper.transform_to_silver_schema(df_raw)

    # Load existing Silver + circuit for FK lookup
    try:
        existing = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    except Exception:
        existing = pd.DataFrame()

    dim_circuit    = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_circuit/dim_circuit.parquet')
    circuit_lookup = dict(zip(dim_circuit['circuit_references'], dim_circuit['circuit_key']))

    # Resolve circuit_key from circuit_ref
    if 'circuit_ref' in df_new.columns:
        df_new['circuit_key'] = df_new['circuit_ref'].map(circuit_lookup)

    # Identify new races by composite natural key
    if not existing.empty:
        existing_composites = set(
            existing['race_year'].astype(str) + '_' + existing['race_round'].astype(str)
        )
    else:
        existing_composites = set()

    df_new['_composite'] = df_new['race_year'].astype(str) + '_' + df_new['race_round'].astype(str)
    df_truly_new = df_new[~df_new['_composite'].isin(existing_composites)].copy().reset_index(drop=True)
    df_truly_new = df_truly_new.drop(columns=['_composite', 'circuit_ref'], errors='ignore')

    if df_truly_new.empty:
        print("  No new races found this week")
        context['ti'].xcom_push(key='races_published', value=0)
        return {'new_count': 0}

    max_key = int(existing['race_key'].max()) if not existing.empty else 0
    max_id  = _safe_max_id(existing, 'race_id', 'f1_data.csv', 10000)

    df_truly_new['race_key']    = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['race_id']     = range(max_id  + 1, max_id  + len(df_truly_new) + 1)
    df_truly_new['source']      = 'api_incremental'
    df_truly_new['created_at']  = datetime.now()

    column_order = [
        'race_key', 'race_id', 'circuit_key', 'race_name', 'race_year', 'race_round',
        'race_date', 'race_time', 'fp1_date', 'fp1_time', 'fp2_date', 'fp2_time',
        'fp3_date', 'fp3_time', 'qualification_date', 'qualification_time',
        'sprint_date', 'sprint_time', 'race_url', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    merged = pd.concat([existing, df_truly_new], ignore_index=True) if not existing.empty else df_truly_new
    minio.upload_parquet(merged, SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    print(f"  Silver updated: {len(merged)} total races")

    count = _publish_to_kafka(df_truly_new, TOPIC_RACES, 'race_id', 'races')
    context['ti'].xcom_push(key='races_published', value=count)
    return {'new_count': count}


# ═══════════════════════════════════════════════════════════════════════════════
# FACT TASKS
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_publish_results(**context):
    """
    Scrape 2026+ race results, resolve FKs, merge Silver (year-partitioned), publish to Kafka.
    New result = result_id not in existing 2026+ Silver partitions.
    """
    print("=" * 70)
    print("PRODUCER: fact_race_results  (2026+)")
    print("=" * 70)

    minio        = MinIOHelper()
    scraper      = ResultScraper()
    current_year = _current_year()

    df_raw = scraper.scrape_results_range(INCREMENTAL_YEAR_START, current_year)
    df_new = scraper.transform_to_silver_schema(df_raw)

    if df_new.empty:
        print("  No results data found")
        context['ti'].xcom_push(key='results_published', value=0)
        return {'new_count': 0}

    # FK resolution
    lookups = _load_dimension_lookups(minio)
    df_new['driver_key']      = df_new['driver_ref'].map(lookups['driver_lookup'])
    df_new['constructor_key'] = df_new['constructor_ref'].map(lookups['constructor_lookup'])
    df_new['_composite']      = df_new['race_year'].astype(str) + '_' + df_new['race_round'].astype(str)
    df_new['race_key']        = df_new['_composite'].map(lookups['race_lookup'])
    df_new['_race_date']      = df_new['race_key'].map(lookups['race_to_date'])
    df_new['time_key']        = df_new['_race_date'].map(lookups['time_map'])
    df_new = df_new.drop(columns=['driver_ref', 'constructor_ref', 'race_year', 'race_round',
                                   '_composite', '_race_date'], errors='ignore')

    # Load existing 2026+ Silver partitions
    all_existing = []
    for yr in range(INCREMENTAL_YEAR_START, current_year + 1):
        try:
            all_existing.append(minio.read_parquet(SILVER_BUCKET, f'facts/fact_race_results/year={yr}/data.parquet'))
        except Exception:
            pass

    existing    = pd.concat(all_existing, ignore_index=True) if all_existing else pd.DataFrame()
    existing_ids = set(existing['result_id'].values) if not existing.empty else set()
    max_key      = int(existing['result_key'].max()) if not existing.empty else 0

    df_truly_new = df_new[~df_new['result_id'].isin(existing_ids)].copy().reset_index(drop=True)

    if df_truly_new.empty:
        print("  No new results found this week")
        context['ti'].xcom_push(key='results_published', value=0)
        return {'new_count': 0}

    df_truly_new['result_key']  = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['source']      = 'api_incremental'
    df_truly_new['created_at']  = datetime.now()

    column_order = [
        'result_key', 'result_id', 'race_key', 'driver_key', 'constructor_key', 'time_key',
        'year', 'driver_number', 'grid', 'position', 'position_text', 'position_order',
        'points', 'number_of_laps', 'rank', 'race_duration_hours', 'race_duration_milliseconds',
        'fastest_lap', 'fastest_lap_minutes', 'fastest_lap_speed', 'status', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    # Save year-partitioned to Silver
    for yr, group in df_truly_new.groupby('year'):
        existing_yr   = existing[existing['year'] == yr] if not existing.empty else pd.DataFrame()
        merged_yr     = pd.concat([existing_yr, group], ignore_index=True) if not existing_yr.empty else group
        minio.upload_parquet(merged_yr, SILVER_BUCKET, f'facts/fact_race_results/year={int(yr)}/data.parquet')
        print(f"  Silver updated: year={int(yr)}, {len(group)} new results")

    count = _publish_to_kafka(df_truly_new, TOPIC_RESULTS, 'result_id', 'results')
    context['ti'].xcom_push(key='results_published', value=count)
    return {'new_count': count}


def scrape_publish_laps(**context):
    """
    Scrape 2026+ lap times, resolve FKs, merge Silver (year-partitioned), publish to Kafka.
    New lap = (race_key + driver_key + lap) not in existing Silver.
    Note: LapScraper.transform_to_silver_schema returns list of dicts (not DataFrame).
    """
    print("=" * 70)
    print("PRODUCER: fact_laps  (2026+)")
    print("=" * 70)

    minio        = MinIOHelper()
    scraper      = LapScraper()
    current_year = _current_year()

    df_raw    = scraper.scrape_laps_range(INCREMENTAL_YEAR_START, current_year)
    raw_list  = scraper.transform_to_silver_schema(df_raw)  # returns list of dicts

    if not raw_list:
        print("  No lap data found")
        context['ti'].xcom_push(key='laps_published', value=0)
        return {'new_count': 0}

    df_new = pd.DataFrame(raw_list)

    # FK resolution (laps have no constructor_ref)
    lookups = _load_dimension_lookups(minio)
    df_new['driver_key'] = df_new['driver_ref'].map(lookups['driver_lookup'])
    df_new['_composite'] = df_new['race_year'].astype(str) + '_' + df_new['race_round'].astype(str)
    df_new['race_key']   = df_new['_composite'].map(lookups['race_lookup'])
    df_new['_race_date'] = df_new['race_key'].map(lookups['race_to_date'])
    df_new['time_key']   = df_new['_race_date'].map(lookups['time_map'])
    df_new['year']       = df_new['race_key'].map(lookups['race_year_map'])
    df_new = df_new.drop(columns=['driver_ref', 'race_year', 'race_round',
                                   '_composite', '_race_date'], errors='ignore')

    # Load existing 2026+ Silver partitions
    all_existing = []
    for yr in range(INCREMENTAL_YEAR_START, current_year + 1):
        try:
            all_existing.append(minio.read_parquet(SILVER_BUCKET, f'facts/fact_laps/year={yr}/data.parquet'))
        except Exception:
            pass

    existing = pd.concat(all_existing, ignore_index=True) if all_existing else pd.DataFrame()

    # Dedup key: race_key + driver_key + lap
    if not existing.empty:
        existing_keys = set(zip(
            existing['race_key'].astype(str),
            existing['driver_key'].astype(str),
            existing['lap'].astype(str)
        ))
        df_new['_dedup'] = list(zip(
            df_new['race_key'].astype(str),
            df_new['driver_key'].astype(str),
            df_new['lap'].astype(str)
        ))
        df_truly_new = df_new[~df_new['_dedup'].isin(existing_keys)].drop(columns=['_dedup']).copy()
    else:
        df_truly_new = df_new.copy()

    if df_truly_new.empty:
        print("  No new laps found this week")
        context['ti'].xcom_push(key='laps_published', value=0)
        return {'new_count': 0}

    max_key = int(existing['lap_key'].max()) if not existing.empty else 0
    df_truly_new = df_truly_new.reset_index(drop=True)
    df_truly_new['lap_key']     = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['source']      = 'api_incremental'
    df_truly_new['created_at']  = datetime.now()

    column_order = [
        'lap_key', 'race_key', 'lap', 'driver_key', 'time_key',
        'lap_position', 'lap_minutes', 'lap_milliseconds', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    # Save year-partitioned to Silver
    for yr, group in df_truly_new.groupby(df_truly_new['race_key'].map(lookups['race_year_map'])):
        existing_yr = existing[existing['race_key'].isin(group['race_key'])] if not existing.empty else pd.DataFrame()
        merged_yr   = pd.concat([existing_yr, group], ignore_index=True) if not existing_yr.empty else group
        minio.upload_parquet(merged_yr, SILVER_BUCKET, f'facts/fact_laps/year={int(yr)}/data.parquet')
        print(f"  Silver updated: year={int(yr)}, {len(group)} new laps")

    count = _publish_to_kafka(df_truly_new, TOPIC_LAPS, 'lap_key', 'laps')
    context['ti'].xcom_push(key='laps_published', value=count)
    return {'new_count': count}


def scrape_publish_pitstops(**context):
    """
    Scrape 2026+ pit stops, resolve FKs, merge Silver, publish to Kafka.
    Renames scraper columns to match Silver schema (stop_number->stop, etc.).
    """
    print("=" * 70)
    print("PRODUCER: fact_pitstops  (2026+)")
    print("=" * 70)

    minio        = MinIOHelper()
    scraper      = PitStopScraper()
    current_year = _current_year()

    df_raw = scraper.scrape_pitstops_range(INCREMENTAL_YEAR_START, current_year)
    df_new = scraper.transform_to_silver_schema(df_raw)

    if df_new.empty:
        print("  No pitstop data found")
        context['ti'].xcom_push(key='pitstops_published', value=0)
        return {'new_count': 0}

    # Rename to match Silver schema (same as api_pitstops_bronze_to_silver.py)
    df_new = df_new.rename(columns={
        'stop_number':      'stop',
        'pit_time':         'local_time',
        'duration_seconds': 'pitlane_duration_seconds'
    })
    df_new['pitlane_duration_milliseconds'] = (
        pd.to_numeric(df_new['pitlane_duration_seconds'], errors='coerce') * 1000
    ).round().astype('Int64')

    # FK resolution
    lookups = _load_dimension_lookups(minio)
    df_new['driver_key'] = df_new['driver_ref'].map(lookups['driver_lookup'])
    df_new['_composite'] = df_new['race_year'].astype(str) + '_' + df_new['race_round'].astype(str)
    df_new['race_key']   = df_new['_composite'].map(lookups['race_lookup'])
    df_new['_race_date'] = df_new['race_key'].map(lookups['race_to_date'])
    df_new['time_key']   = df_new['_race_date'].map(lookups['time_map'])
    df_new = df_new.drop(columns=['driver_ref', 'race_year', 'race_round',
                                   '_composite', '_race_date'], errors='ignore')

    # Load existing 2026+ Silver partitions
    all_existing = []
    for yr in range(INCREMENTAL_YEAR_START, current_year + 1):
        try:
            all_existing.append(minio.read_parquet(SILVER_BUCKET, f'facts/fact_pitstops/year={yr}/data.parquet'))
        except Exception:
            pass

    existing = pd.concat(all_existing, ignore_index=True) if all_existing else pd.DataFrame()

    # Dedup key: race_key + driver_key + stop
    if not existing.empty:
        existing_keys = set(zip(
            existing['race_key'].astype(str),
            existing['driver_key'].astype(str),
            existing['stop'].astype(str)
        ))
        df_new['_dedup'] = list(zip(
            df_new['race_key'].astype(str),
            df_new['driver_key'].astype(str),
            df_new['stop'].astype(str)
        ))
        df_truly_new = df_new[~df_new['_dedup'].isin(existing_keys)].drop(columns=['_dedup']).copy()
    else:
        df_truly_new = df_new.copy()

    if df_truly_new.empty:
        print("  No new pitstops found this week")
        context['ti'].xcom_push(key='pitstops_published', value=0)
        return {'new_count': 0}

    max_key = int(existing['pitstop_key'].max()) if not existing.empty else 0
    df_truly_new = df_truly_new.reset_index(drop=True)
    df_truly_new['pitstop_key']  = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['source']       = 'api_incremental'
    df_truly_new['created_at']   = datetime.now()

    column_order = [
        'pitstop_key', 'race_key', 'driver_key', 'stop', 'time_key',
        'lap', 'local_time', 'pitlane_duration_seconds', 'pitlane_duration_milliseconds',
        'year', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    for yr, group in df_truly_new.groupby('year'):
        existing_yr = existing[existing['year'] == yr] if not existing.empty else pd.DataFrame()
        merged_yr   = pd.concat([existing_yr, group], ignore_index=True) if not existing_yr.empty else group
        minio.upload_parquet(merged_yr, SILVER_BUCKET, f'facts/fact_pitstops/year={int(yr)}/data.parquet')
        print(f"  Silver updated: year={int(yr)}, {len(group)} new pitstops")

    count = _publish_to_kafka(df_truly_new, TOPIC_PITSTOPS, 'pitstop_key', 'pitstops')
    context['ti'].xcom_push(key='pitstops_published', value=count)
    return {'new_count': count}


def scrape_publish_driver_standings(**context):
    """
    Scrape 2026+ driver standings, resolve FKs, merge Silver, publish to Kafka.
    New standing = (race_key + driver_key) not in existing Silver.
    """
    print("=" * 70)
    print("PRODUCER: fact_driver_standings  (2026+)")
    print("=" * 70)

    minio        = MinIOHelper()
    scraper      = DriverStandingScraper()
    current_year = _current_year()

    df_raw = scraper.scrape_dstandings_range(INCREMENTAL_YEAR_START, current_year)
    df_new = scraper.transform_to_silver_schema(df_raw)

    if df_new.empty:
        print("  No driver standings data found")
        context['ti'].xcom_push(key='dstandings_published', value=0)
        return {'new_count': 0}

    # FK resolution
    lookups = _load_dimension_lookups(minio)
    df_new['driver_key']      = df_new['driver_ref'].map(lookups['driver_lookup'])
    df_new['constructor_key'] = df_new['constructor_ref'].map(lookups['constructor_lookup'])
    df_new['_composite']      = df_new['race_year'].astype(str) + '_' + df_new['race_round'].astype(str)
    df_new['race_key']        = df_new['_composite'].map(lookups['race_lookup'])
    df_new['_race_date']      = df_new['race_key'].map(lookups['race_to_date'])
    df_new['time_key']        = df_new['_race_date'].map(lookups['time_map'])
    df_new = df_new.drop(columns=['driver_ref', 'constructor_ref', 'race_year', 'race_round',
                                   '_composite', '_race_date'], errors='ignore')

    # Load existing Silver (non-partitioned)
    try:
        existing = minio.read_parquet(SILVER_BUCKET, 'facts/fact_driver_standings/fact_driver_standings.parquet')
    except Exception:
        existing = pd.DataFrame()

    # Dedup key: race_key + driver_key
    if not existing.empty:
        existing_keys = set(zip(existing['race_key'].astype(str), existing['driver_key'].astype(str)))
        df_new['_dedup'] = list(zip(df_new['race_key'].astype(str), df_new['driver_key'].astype(str)))
        df_truly_new = df_new[~df_new['_dedup'].isin(existing_keys)].drop(columns=['_dedup']).copy()
    else:
        df_truly_new = df_new.copy()

    if df_truly_new.empty:
        print("  No new driver standings found this week")
        context['ti'].xcom_push(key='dstandings_published', value=0)
        return {'new_count': 0}

    max_key = int(existing['ds_key'].max()) if not existing.empty else 0
    max_id  = int(existing['ds_id'].max()) if not existing.empty else 0
    df_truly_new = df_truly_new.reset_index(drop=True)
    df_truly_new['ds_key']     = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['ds_id']      = range(max_id  + 1, max_id  + len(df_truly_new) + 1)
    df_truly_new['source']     = 'api_incremental'
    df_truly_new['created_at'] = datetime.now()

    column_order = [
        'ds_key', 'ds_id', 'race_key', 'driver_key', 'constructor_key', 'time_key',
        'ds_points', 'ds_position', 'ds_wins', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    merged = pd.concat([existing, df_truly_new], ignore_index=True) if not existing.empty else df_truly_new
    minio.upload_parquet(merged, SILVER_BUCKET, 'facts/fact_driver_standings/fact_driver_standings.parquet')
    print(f"  Silver updated: {len(merged)} total driver standings")

    count = _publish_to_kafka(df_truly_new, TOPIC_DRIVER_STANDINGS, 'ds_key', 'driver standings')
    context['ti'].xcom_push(key='dstandings_published', value=count)
    return {'new_count': count}


def scrape_publish_constructor_standings(**context):
    """
    Scrape 2026+ constructor standings, resolve FKs, merge Silver, publish to Kafka.
    New standing = (race_key + constructor_key) not in existing Silver.
    """
    print("=" * 70)
    print("PRODUCER: fact_constructor_standings  (2026+)")
    print("=" * 70)

    minio        = MinIOHelper()
    scraper      = ConstructorStandingScraper()
    current_year = _current_year()

    df_raw = scraper.scrape_cstandings_range(INCREMENTAL_YEAR_START, current_year)
    df_new = scraper.transform_to_silver_schema(df_raw)

    if df_new.empty:
        print("  No constructor standings data found")
        context['ti'].xcom_push(key='cstandings_published', value=0)
        return {'new_count': 0}

    # FK resolution
    lookups = _load_dimension_lookups(minio)
    df_new['constructor_key'] = df_new['constructor_ref'].map(lookups['constructor_lookup'])
    df_new['_composite']      = df_new['race_year'].astype(str) + '_' + df_new['race_round'].astype(str)
    df_new['race_key']        = df_new['_composite'].map(lookups['race_lookup'])
    df_new['_race_date']      = df_new['race_key'].map(lookups['race_to_date'])
    df_new['time_key']        = df_new['_race_date'].map(lookups['time_map'])
    df_new = df_new.drop(columns=['constructor_ref', 'race_year', 'race_round',
                                   '_composite', '_race_date'], errors='ignore')

    # Load existing Silver (non-partitioned)
    try:
        existing = minio.read_parquet(SILVER_BUCKET, 'facts/fact_constructor_standings/fact_constructor_standings.parquet')
    except Exception:
        existing = pd.DataFrame()

    # Dedup key: race_key + constructor_key
    if not existing.empty:
        existing_keys = set(zip(existing['race_key'].astype(str), existing['constructor_key'].astype(str)))
        df_new['_dedup'] = list(zip(df_new['race_key'].astype(str), df_new['constructor_key'].astype(str)))
        df_truly_new = df_new[~df_new['_dedup'].isin(existing_keys)].drop(columns=['_dedup']).copy()
    else:
        df_truly_new = df_new.copy()

    if df_truly_new.empty:
        print("  No new constructor standings found this week")
        context['ti'].xcom_push(key='cstandings_published', value=0)
        return {'new_count': 0}

    max_key = int(existing['cs_key'].max()) if not existing.empty else 0
    max_id  = int(existing['cs_id'].max()) if not existing.empty else 0
    df_truly_new = df_truly_new.reset_index(drop=True)
    df_truly_new['cs_key']     = range(max_key + 1, max_key + len(df_truly_new) + 1)
    df_truly_new['cs_id']      = range(max_id  + 1, max_id  + len(df_truly_new) + 1)
    df_truly_new['source']     = 'api_incremental'
    df_truly_new['created_at'] = datetime.now()

    column_order = [
        'cs_key', 'cs_id', 'race_key', 'constructor_key', 'time_key',
        'cs_points', 'cs_position', 'cs_wins', 'created_at', 'source'
    ]
    df_truly_new = df_truly_new[[c for c in column_order if c in df_truly_new.columns]]

    merged = pd.concat([existing, df_truly_new], ignore_index=True) if not existing.empty else df_truly_new
    minio.upload_parquet(merged, SILVER_BUCKET, 'facts/fact_constructor_standings/fact_constructor_standings.parquet')
    print(f"  Silver updated: {len(merged)} total constructor standings")

    count = _publish_to_kafka(df_truly_new, TOPIC_CONSTRUCTOR_STANDINGS, 'cs_key', 'constructor standings')
    context['ti'].xcom_push(key='cstandings_published', value=count)
    return {'new_count': count}


def print_producer_summary(**context):
    ti = context['ti']
    print("\n" + "=" * 70)
    print("KAFKA PRODUCER PIPELINE COMPLETE")
    print("=" * 70)
    for key, label in [
        ('drivers_published',       'Drivers'),
        ('constructors_published',  'Constructors'),
        ('circuits_published',      'Circuits'),
        ('races_published',         'Races'),
        ('results_published',       'Results'),
        ('laps_published',          'Laps'),
        ('pitstops_published',      'Pitstops'),
        ('dstandings_published',    'Driver standings'),
        ('cstandings_published',    'Constructor standings'),
    ]:
        count = ti.xcom_pull(key=key) or 0
        print(f"  {label:<25s}: {count} new records published")
    print("=" * 70)


# ═══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id='kafka_producer_pipeline',
    default_args=default_args,
    description='F1 Incremental Producer: Ergast API (2026+) -> Silver -> Kafka Topics',
    schedule='0 8 * * 1',       # Every Monday at 08:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['kafka', 'producer', 'incremental', 'f1-etl'],
) as dag:

    # ── Dimension tasks (run in parallel) ────────────────────────────────────
    task_drivers = PythonOperator(
        task_id='scrape_publish_drivers',
        python_callable=scrape_publish_drivers,
        doc='Scrape 2026+ drivers, merge Silver, publish new records to f1.dim.drivers'
    )

    task_constructors = PythonOperator(
        task_id='scrape_publish_constructors',
        python_callable=scrape_publish_constructors,
        doc='Scrape 2026+ constructors, merge Silver, publish new records to f1.dim.constructors'
    )

    task_circuits = PythonOperator(
        task_id='scrape_publish_circuits',
        python_callable=scrape_publish_circuits,
        doc='Fetch all-time circuits, find new venues, publish to f1.dim.circuits'
    )

    task_races = PythonOperator(
        task_id='scrape_publish_races',
        python_callable=scrape_publish_races,
        doc='Scrape 2026+ races, resolve circuit_key, publish new records to f1.dim.races'
    )

    # ── Fact tasks (run in parallel after all dims are ready) ────────────────
    task_results = PythonOperator(
        task_id='scrape_publish_results',
        python_callable=scrape_publish_results,
        doc='Scrape 2026+ results, resolve FKs, publish new records to f1.fact.results'
    )

    task_laps = PythonOperator(
        task_id='scrape_publish_laps',
        python_callable=scrape_publish_laps,
        doc='Scrape 2026+ laps, resolve FKs, publish new records to f1.fact.laps'
    )

    task_pitstops = PythonOperator(
        task_id='scrape_publish_pitstops',
        python_callable=scrape_publish_pitstops,
        doc='Scrape 2026+ pitstops, resolve FKs, publish new records to f1.fact.pitstops'
    )

    task_dstandings = PythonOperator(
        task_id='scrape_publish_driver_standings',
        python_callable=scrape_publish_driver_standings,
        doc='Scrape 2026+ driver standings, resolve FKs, publish to f1.fact.driver_standings'
    )

    task_cstandings = PythonOperator(
        task_id='scrape_publish_constructor_standings',
        python_callable=scrape_publish_constructor_standings,
        doc='Scrape 2026+ constructor standings, resolve FKs, publish to f1.fact.constructor_standings'
    )

    task_summary = PythonOperator(
        task_id='print_summary',
        python_callable=print_producer_summary,
        doc='Log how many records were published per topic'
    )

    task_trigger_consumer = TriggerDagRunOperator(
        task_id='trigger_consumer_dag',
        trigger_dag_id='kafka_consumer_pipeline',
        wait_for_completion=False,   # fire-and-forget; consumer runs independently
        doc='Trigger consumer DAG to load Kafka messages into PostgreSQL Gold'
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    # circuits must finish before races (races need circuit_key from Silver)
    # drivers and constructors run in parallel with circuits
    task_circuits >> task_races
    independent_dims = [task_drivers, task_constructors, task_circuits]

    # All facts depend on ALL dims (need FKs from dim files)
    facts = [task_results, task_laps, task_pitstops, task_dstandings, task_cstandings]
    for dim in independent_dims:
        dim >> facts
    task_races >> facts

    # Summary + trigger after all facts complete
    facts >> task_summary >> task_trigger_consumer
