# dags/api_pitstops_bronze_to_silver.py

"""
F1 Pit Stops Backfill Pipeline: Ergast API (2024-2025) -> Silver

Architecture:
- pitstop_key (INT): Surrogate key (Silver internal, sequential 1, 2, 3...)
- NO natural key (composite: race + driver + stop_number)

Merge Logic:
- Match on: race_year + race_round + driver_ref + stop_number
- CSV has priority (skip API update for existing CSV pit stops)
- Need FK lookup: driver_ref, race_year+race_round

Data Flow:
1. Scrape API (2024-2025)
2. Transform to Silver schema
3. Load dimensions for FK lookup (driver, race, constructor, time)
4. Resolve all FKs (driver_key, race_key, constructor_key, time_key)
5. Load existing Silver (CSV pit stops)
6. Merge (incremental, idempotent)
7. Write to Silver (partitioned by year)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.s3_helper import MinIOHelper
from scraper.pitstop import PitStopScraper

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

SILVER_BUCKET = 'f1-silver'
BACKFILL_YEAR_START = 2024
BACKFILL_YEAR_END = 2025


# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def pitstops_equal(row1: pd.Series, row2: pd.Series) -> bool:
    """
    Check if two pit stop rows are identical (for idempotency)
    
    Args:
        row1: Existing pit stop row (from Silver)
        row2: New pit stop row (from API)
    
    Returns:
        True if identical, False if any field differs
    """
    compare_fields = [
        'stop_number',
        'lap',
        'pit_time',
        'duration_seconds'
    ]
    
    for field in compare_fields:
        val1, val2 = row1.get(field), row2.get(field)
        
        # Both NULL → equal
        if pd.isna(val1) and pd.isna(val2):
            continue
        
        # One is NULL, other is not → different
        if pd.isna(val1) or pd.isna(val2):
            return False
        
        # Both have values → compare
        if val1 != val2:
            return False
    
    return True


def merge_pitstops(existing: pd.DataFrame, new_api: pd.DataFrame) -> pd.DataFrame:
    """
    Merge existing Silver data with new API data (idempotent)
    
    Merge Strategy:
    1. Match on: race_year + race_round + driver_ref + stop_number (composite key)
    2. CSV has priority (skip API for existing CSV pit stops)
    3. API pit stops update if data changed
    4. New API pit stops are added
    
    Args:
        existing: Existing Silver DataFrame (CSV + previous API)
        new_api: New API DataFrame (transformed to Silver schema)
    
    Returns:
        Merged DataFrame (idempotent, deterministic)
    """
    print("\n  Merge Configuration:")
    print(f"Existing pit stops: {len(existing):,}")
    print(f"New API pit stops: {len(new_api)}")
    print(f"Merge key: race_key + driver_key + stop_number")
    print(f"CSV priority: YES (skip API for CSV pit stops)")

    # EDGE CASE: First run (no existing data)
    if existing.empty:
        print("\n  FIRST RUN: No existing data, adding all API pit stops")
        new_api = new_api.copy()
        new_api['pitstop_key'] = range(1, len(new_api) + 1)
        print(f"  Generated pitstop_key: 1-{len(new_api)}")
        return new_api

    # MAIN CASE: Incremental merge
    print(f"\n  Processing {len(new_api)} API pit stops:\n")

    # Create composite match keys using FK columns (race_year/driver_ref are dropped from Silver)
    existing['_match_key'] = (
        existing['race_key'].astype(str) + '_' +
        existing['driver_key'].astype(str) + '_' +
        existing['stop'].astype(str)
    )

    new_api['_match_key'] = (
        new_api['race_key'].astype(str) + '_' +
        new_api['driver_key'].astype(str) + '_' +
        new_api['stop'].astype(str)
    )
    
    # Keep CSV pit stops (priority)
    csv_stops = existing[existing['source'].str.contains('f1_data.csv', case=False, na=False)].copy()
    csv_match_keys = set(csv_stops['_match_key'])
    
    # Filter API stops: exclude those that match CSV
    new_api_filtered = new_api[~new_api['_match_key'].isin(csv_match_keys)].copy()
    
    print(f"  CSV pit stops (preserved): {len(csv_stops):,}")
    print(f"  API pit stops (skipped - CSV exists): {len(new_api) - len(new_api_filtered):,}")
    print(f"  API pit stops (to add): {len(new_api_filtered):,}")
    
    # Combine CSV + new API stops
    merged = pd.concat([csv_stops, new_api_filtered], ignore_index=True)

    # Drop temporary match key
    merged = merged.drop(columns=['_match_key'], errors='ignore')

    # Regenerate pitstop_key (sequential)
    merged = merged.sort_values(['year', 'race_key', 'stop']).reset_index(drop=True)
    merged['pitstop_key'] = range(1, len(merged) + 1)
    
    return merged


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE FUNCTION
# ═══════════════════════════════════════════════════════════════

def backfill_fact_pitstops(**context):
    """
    Backfill fact_pitstops from Ergast API
    
    Pipeline Steps:
    1. Scrape API (2024-2025)
    2. Transform to Silver schema
    3. Load dimensions for FK lookup (driver, race, constructor, time)
    4. Resolve all FKs (driver_key, race_key, constructor_key, time_key)
    5. Load existing Silver (CSV pit stops)
    6. Merge (incremental, idempotent)
    7. Write to Silver (partitioned by year)
    
    Returns:
        Dict with statistics
    """
    print("=" * 70)
    print("F1 PIT STOPS BACKFILL: ERGAST API -> SILVER")
    print("=" * 70)
    
    minio = MinIOHelper()
    scraper = PitStopScraper()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: SCRAPE API
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 1] Scraping API...")
    
    df_api_raw = scraper.scrape_pitstops_range(
        year_start=BACKFILL_YEAR_START,
        year_end=BACKFILL_YEAR_END
    )
    
    print(f"API scraping complete: {len(df_api_raw):,} pit stop records")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: TRANSFORM to Silver schema
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 2] Transforming to Silver schema...")
    
    df_api_transformed = scraper.transform_to_silver_schema(df_api_raw)
    
    print(f"Transformation complete: {len(df_api_transformed)} pit stops")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 3: LOAD dimensions for FK lookup
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 3] Loading dimensions for FK resolution...")
    
    try:
        dim_driver = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_driver/dim_driver.parquet'
        )
        dim_constructor = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_constructor/dim_constructor.parquet'
        )
        dim_race = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_race/dim_race.parquet'
        )
        dim_time = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_time/dim_time.parquet'
        )
        
        print(f"Loaded dimensions:")
        print(f"  - dim_driver: {len(dim_driver):,} drivers")
        print(f"  - dim_constructor: {len(dim_constructor):,} constructors")
        print(f"  - dim_race: {len(dim_race):,} races")
        print(f"  - dim_time: {len(dim_time):,} dates")
    
    except Exception as e:
        print(f"ERROR: Cannot load dimensions - FK resolution impossible!")
        print(f"Details: {e}")
        raise RuntimeError("All dimensions must exist before loading pit stops") from e
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 4: RESOLVE FKs
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 4] Resolving foreign keys...")
    
    # Create lookup dictionaries
    driver_lookup = dict(zip(dim_driver['driver_ref'], dim_driver['driver_key']))
    constructor_lookup = dict(zip(dim_constructor['constructor_ref'], dim_constructor['constructor_key']))
    
    # Race lookup: composite key (year + round) → race_key
    dim_race['race_composite'] = dim_race['race_year'].astype(str) + '_' + dim_race['race_round'].astype(str)
    race_lookup = dict(zip(dim_race['race_composite'], dim_race['race_key']))
    
    # Time lookup: race_key → race_date → time_key
    race_to_date = dict(zip(dim_race['race_key'], dim_race['race_date']))
    time_map = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))
    
    # Constructor lookup from race results: (year, round, driver_ref) → constructor_ref
    print("  Building race-driver-constructor mapping from race results...")
    race_to_constructor = {}
    try:
        for year in range(BACKFILL_YEAR_START, BACKFILL_YEAR_END + 1):
            try:
                results_df = minio.read_parquet(
                    SILVER_BUCKET,
                    f'facts/fact_race_results/year={year}/data.parquet'
                )
                
                # Build reverse mappings
                race_key_to_year_round = {}
                for _, race_row in dim_race.iterrows():
                    race_key_to_year_round[race_row['race_key']] = (race_row['race_year'], race_row['race_round'])
                
                driver_key_to_ref = dict(zip(dim_driver['driver_key'], dim_driver['driver_ref']))
                constructor_key_to_ref = dict(zip(dim_constructor['constructor_key'], dim_constructor['constructor_ref']))
                
                for _, result_row in results_df.iterrows():
                    race_key = result_row['race_key']
                    driver_key = result_row['driver_key']
                    constructor_key = result_row['constructor_key']
                    
                    if race_key in race_key_to_year_round and driver_key in driver_key_to_ref:
                        year_val, round_val = race_key_to_year_round[race_key]
                        driver_ref = driver_key_to_ref[driver_key]
                        constructor_ref = constructor_key_to_ref.get(constructor_key)
                        
                        key = (year_val, round_val, driver_ref)
                        race_to_constructor[key] = constructor_ref
            except Exception:
                pass
        
        print(f"  Built race-driver-constructor mapping: {len(race_to_constructor):,} entries")
    except Exception as e:
        print(f"  WARNING: Could not load race results for constructor mapping: {e}")
        print("  Pit stops will have NULL constructor_key")
    
    print(f"Created lookup dictionaries:")
    print(f"  - driver_ref -> driver_key: {len(driver_lookup)} entries")
    print(f"  - race_year+race_round -> race_key: {len(race_lookup)} entries")
    print(f"  - race_date -> time_key: {len(time_map)} entries")
    
    # Resolve driver_key
    df_api_transformed['driver_key'] = df_api_transformed['driver_ref'].map(driver_lookup)
    unresolved_drivers = df_api_transformed['driver_key'].isna().sum()
    if unresolved_drivers > 0:
        print(f"WARNING: {unresolved_drivers} pit stops have unresolved driver_ref!")
    
    # Resolve race_key (composite: year + round)
    df_api_transformed['race_composite'] = (
        df_api_transformed['race_year'].astype(str) + '_' + 
        df_api_transformed['race_round'].astype(str)
    )
    df_api_transformed['race_key'] = df_api_transformed['race_composite'].map(race_lookup)
    unresolved_races = df_api_transformed['race_key'].isna().sum()
    if unresolved_races > 0:
        print(f"WARNING: {unresolved_races} pit stops have unresolved race (year+round)!")
    
    # Resolve constructor_key (from race results mapping)
    df_api_transformed['constructor_key'] = df_api_transformed.apply(
        lambda row: constructor_lookup.get(
            race_to_constructor.get((row['race_year'], row['race_round'], row['driver_ref']))
        ),
        axis=1
    )
    unresolved_constructors = df_api_transformed['constructor_key'].isna().sum()
    if unresolved_constructors > 0:
        print(f"WARNING: {unresolved_constructors} pit stops have unresolved constructor_key!")
    
    # Resolve time_key (race_key → race_date → time_key)
    df_api_transformed['race_date_temp'] = df_api_transformed['race_key'].map(race_to_date)
    df_api_transformed['time_key'] = df_api_transformed['race_date_temp'].map(time_map)
    unresolved_time = df_api_transformed['time_key'].isna().sum()
    if unresolved_time > 0:
        print(f"WARNING: {unresolved_time} pit stops have unresolved time_key!")
    
    
    df_api_transformed = df_api_transformed.drop(columns=[
        'race_composite',
        'race_date_temp'
    ], errors='ignore')

    # Rename API columns to match CSV Silver schema so merge and Gold loader see one schema
    df_api_transformed = df_api_transformed.rename(columns={
        'stop_number': 'stop',
        'pit_time': 'local_time',
        'duration_seconds': 'pitlane_duration_seconds'
    })
    # Add milliseconds column (API only provides seconds)
    df_api_transformed['pitlane_duration_milliseconds'] = (
        pd.to_numeric(df_api_transformed['pitlane_duration_seconds'], errors='coerce') * 1000
    ).round().astype('Int64')

    print(f"All FKs resolved successfully")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 5: LOAD existing Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 5] Loading existing Silver data...")
    
    try:
        # Load ALL existing year partitions (CSV writes all years, not just backfill years)
        all_existing = []
        partitions = minio.list_objects(SILVER_BUCKET, 'facts/fact_pitstops/')
        for partition_path in partitions:
            if partition_path.endswith('.parquet'):
                try:
                    year_data = minio.read_parquet(SILVER_BUCKET, partition_path)
                    all_existing.append(year_data)
                except Exception:
                    continue

        if all_existing:
            existing_silver = pd.concat(all_existing, ignore_index=True)
            print(f"Loaded {len(existing_silver):,} existing pit stops from Silver")
            source_counts = existing_silver['source'].value_counts().to_dict()
            print(f"Existing sources: {source_counts}")
        else:
            print(f"No existing Silver data found (first run)")
            existing_silver = pd.DataFrame()

    except Exception as e:
        print(f"No existing Silver data found (first run)")
        print(f"Details: {e}")
        existing_silver = pd.DataFrame()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 6: MERGE (incremental, idempotent)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 6] Merging API data with existing Silver...")
    
    merged = merge_pitstops(existing_silver, df_api_transformed)
    
    print(f"Merge complete: {len(merged):,} total pit stops")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 7: WRITE to Silver (partitioned by year)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 7] Writing to Silver layer (partitioned by year)...")
    
   
    columns_to_drop = ['driver_ref', 'race_year', 'race_round']
    for col in columns_to_drop:
        if col in merged.columns:
            merged = merged.drop(columns=[col])

    # Ensure correct column order (matches CSV Silver and Gold expectations)
    column_order = [
        'pitstop_key',
        'race_key',
        'driver_key',
        'constructor_key',
        'time_key',
        'stop',
        'lap',
        'local_time',
        'pitlane_duration_seconds',
        'pitlane_duration_milliseconds',
        'year',
        'created_at',
        'source'
    ]
    
    # Select only columns that exist
    merged = merged[[col for col in column_order if col in merged.columns]]
    
    # Write partitioned by year
    for year in sorted(merged['year'].unique()):
        year_data = merged[merged['year'] == year]
        object_name = f'facts/fact_pitstops/year={year}/data.parquet'
        minio.upload_parquet(year_data, SILVER_BUCKET, object_name)
        print(f"  Year {year}: {len(year_data):,} pit stops written")
    
    print(f"Written to: s3://{SILVER_BUCKET}/facts/fact_pitstops/year=*/data.parquet")
    
    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    summary = {
        'existing_count': len(existing_silver),
        'api_scraped': len(df_api_transformed),
        'final_count': len(merged),
        'new_pitstops_added': len(merged) - len(existing_silver) if not existing_silver.empty else len(merged)
    }
    
    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"  Existing pit stops in Silver: {summary['existing_count']:,}")
    print(f"  API pit stops scraped: {summary['api_scraped']}")
    print(f"  Final pit stop count: {summary['final_count']:,}")
    print(f"  New pit stops added: {summary['new_pitstops_added']}")
    
    # Source breakdown
    if not merged.empty and 'source' in merged.columns:
        source_final = merged['source'].value_counts().to_dict()
        print(f"\n  Final source breakdown:")
        for source, count in source_final.items():
            print(f"    {source:15s}: {count:,}")
    
    print("=" * 70)
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='backfill_summary', value=summary)
    
    return summary


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='api_pitstops_bronze_to_silver',
    default_args=default_args,
    description='F1 ETL: API Bronze -> Silver | Backfill pit stops 2024-2025',
    schedule=None,
    start_date=datetime(2026, 2, 10),
    catchup=False,
    tags=['silver','fact','minio', 'api'],
) as dag:
    
    task_backfill = PythonOperator(
        task_id='backfill_fact_pitstops',
        python_callable=backfill_fact_pitstops,
        execution_timeout=timedelta(hours=3)
    )
