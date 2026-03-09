# dags/api_results_bronze_to_silver.py

"""
F1 Race Results Backfill Pipeline: Ergast API (2011-2025) -> Silver

Architecture:
- result_key (INT): Surrogate key (Silver internal, sequential 1, 2, 3...)
- result_id (INT): Natural key (CSV: 1-XXX), Pseudo-natural key (API: 10000001+)
- NO composite natural key (result_id is unique across all years)

Merge Logic:
- Match on result_id (primary natural key)
- CSV has priority (skip API update for existing CSV results)
- Need FK lookup: driver_ref, constructor_ref, race_year+race_round

Data Flow:
1. Scrape API (2011-2025)
2. Transform to Silver schema (generate result_id 10000001+)
3. Load dimensions for FK lookup (driver, constructor, race, time)
4. Resolve all FKs (driver_key, constructor_key, race_key, time_key)
5. Load existing Silver (CSV results)
6. Merge (CSV + API)
7. Write to Silver (partitioned by year)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.s3_helper import MinIOHelper
from scraper.result import ResultScraper

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

def results_equal(row1: pd.Series, row2: pd.Series) -> bool:
    """
    Check if two result rows are identical (for idempotency)
    
    Compares business fields (excluding FKs which may differ)
    
    Args:
        row1: Existing result row (from Silver)
        row2: New result row (from API)
    
    Returns:
        True if identical, False if any field differs
    """
    compare_fields = [
        'driver_number',
        'grid',
        'position',
        'position_text',
        'position_order',
        'points',
        'number_of_laps',
        'race_duration_hours',
        'race_duration_milliseconds',
        'fastest_lap',
        'rank',
        'fastest_lap_minutes',
        'fastest_lap_speed',
        'status'
    ]
    
    for field in compare_fields:
        val1, val2 = row1.get(field), row2.get(field)
        
        # Both NULL -> equal, continue
        if pd.isna(val1) and pd.isna(val2):
            continue
        
        # One is NULL, other is not -> different
        if pd.isna(val1) or pd.isna(val2):
            return False
        
        # Both have values -> compare
        if val1 != val2:
            return False
    
    return True


def _should_skip_csv_result(existing_row: pd.Series, result_id: int) -> bool:
    """Check if result should be skipped due to CSV priority"""
    source = str(existing_row['source']).lower()
    if 'f1_data.csv' in source:
        print(f"SKIP: result_id={result_id:8d} (CSV priority)")
        return True
    return False


def _should_skip_duplicate(existing_row: pd.Series, new_row: pd.Series, result_id: int) -> bool:
    """Check if API result is identical and should be skipped"""
    if results_equal(existing_row, new_row):
        print(f"SKIP: result_id={result_id:8d} (duplicate, no change)")
        return True
    return False


def _add_new_result(merged: pd.DataFrame, new_row: pd.Series, max_key: int) -> tuple:
    """Add new API result to merged DataFrame"""
    result_id = new_row['result_id']
    print(f"ADD: result_id={result_id:8d} (new result from API)")
    
    # Generate NEW surrogate key
    new_key = max_key + 1
    new_row['result_key'] = new_key
    new_row['created_at'] = pd.Timestamp.now()
    
    # Append to merged DataFrame
    merged_updated = pd.concat([merged, new_row.to_frame().T], ignore_index=True)
    return new_key, merged_updated


def _update_existing_result(merged: pd.DataFrame, existing_row: pd.Series, 
                           new_row: pd.Series) -> None:
    """Update existing API result with new data"""
    result_id = new_row['result_id']
    print(f"UPDATE: result_id={result_id:8d} (API refresh, data changed)")
    
    # PRESERVE existing metadata
    new_row['result_key'] = existing_row['result_key']
    new_row['result_id'] = existing_row['result_id']
    new_row['source'] = existing_row['source']
    new_row['created_at'] = existing_row['created_at']
    
    # Update row in merged DataFrame
    mask = merged['result_id'] == result_id
    for col in new_row.index:
        merged.loc[mask, col] = new_row[col]


def _process_api_result(merged: pd.DataFrame, new_row: pd.Series, max_key: int) -> tuple:
    """
    Process single API result: skip, update, or add
    
    Returns:
        (updated_max_key, updated_merged_df)
    """
    result_id = new_row['result_id']
    
    # Result doesn't exist - ADD new
    if result_id not in merged['result_id'].values:
        new_key, merged = _add_new_result(merged, new_row, max_key)
        return new_key, merged
    
    # Result exists - check action
    existing_row = merged[merged['result_id'] == result_id].iloc[0]
    
    # CSV has priority - SKIP
    if _should_skip_csv_result(existing_row, result_id):
        return max_key, merged
    
    # API result - check if duplicate
    if _should_skip_duplicate(existing_row, new_row, result_id):
        return max_key, merged
    
    # Data changed - UPDATE
    _update_existing_result(merged, existing_row, new_row)
    return max_key, merged


def merge_results(existing: pd.DataFrame, new_api: pd.DataFrame, global_max_key: int = 0) -> pd.DataFrame:
    """
    Merge existing Silver data with new API data (idempotent)

    Merge Strategy:
    1. Match on result_id (PRIMARY natural key)
    2. CSV has priority (skip API for existing CSV results)
    3. API results update if data changed
    4. New API results are added

    Key Generation:
    - result_key (surrogate): Sequential (1, 2, 3, ...)
    - result_id (natural): CSV (1-XXX) or API (10000001+)

    Args:
        existing: Existing Silver DataFrame (CSV + previous API)
        new_api: New API DataFrame (transformed to Silver schema)
        global_max_key: Global max result_key across ALL year partitions

    Returns:
        Merged DataFrame (idempotent, deterministic)
    """
    print("\n  Merge Configuration:")
    print(f"Existing results: {len(existing):,}")
    print(f"New API results: {len(new_api)}")
    print(f"Merge key: result_id (PRIMARY natural key)")
    print(f"CSV priority: YES (skip API for CSV results)")

    # EDGE CASE: First run (no existing data for 2024-2025)
    if existing.empty:
        print("\n FIRST RUN: No existing data for target years, adding all API results")
        new_api = new_api.copy()
        start_key = global_max_key + 1
        new_api['result_key'] = range(start_key, start_key + len(new_api))
        print(f"Generated result_key: {start_key}-{start_key + len(new_api) - 1} (global_max_key was {global_max_key})")
        print(f"result_id already set: {new_api['result_id'].min()}-{new_api['result_id'].max()}")
        return new_api

    # MAIN CASE: Incremental merge
    merged = existing.copy()
    max_result_key = max(int(merged['result_key'].max()), global_max_key)
    print(f"\n  Current max result_key: {max_result_key} (global_max_key={global_max_key})")
    print(f"\n  Processing {len(new_api)} API results:\n")
    
    # Process each API result
    for idx, new_row in new_api.iterrows():
        max_result_key, merged = _process_api_result(merged, new_row, max_result_key)
    
    # Sort by race, then driver, then result_key for logical grouping
    merged = merged.sort_values(['race_key', 'driver_key', 'result_key','result_id']).reset_index(drop=True)
    
    return merged


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE FUNCTION
# ═══════════════════════════════════════════════════════════════

def backfill_fact_race_results(**context):
    """
    Backfill fact_race_results from Ergast API
    
    Pipeline Steps:
    1. Scrape API (2011-2025)
    2. Transform to Silver schema
    3. Load dimensions for FK lookup
    4. Resolve all FKs (driver_key, constructor_key, race_key, time_key)
    5. Load existing Silver (CSV + previous API data)
    6. Merge (incremental, idempotent)
    7. Write to Silver (partitioned by year)
    
    Returns:
        Dict with statistics
    """
    print("=" * 70)
    print("F1 RACE RESULTS BACKFILL: ERGAST API -> SILVER")
    print("=" * 70)
    
    minio = MinIOHelper()
    scraper = ResultScraper()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: SCRAPE API
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 1] Scraping API...")
    
    df_api_raw = scraper.scrape_results_range(
        year_start=BACKFILL_YEAR_START,
        year_end=BACKFILL_YEAR_END
    )
    
    print(f"API scraping complete: {len(df_api_raw):,} result records")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: TRANSFORM to Silver schema
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 2] Transforming to Silver schema...")
    
    df_api_transformed = scraper.transform_to_silver_schema(df_api_raw)
    
    print(f"Transformation complete: {len(df_api_transformed)} results")
    print(f"result_id range: {df_api_transformed['result_id'].min()} - {df_api_transformed['result_id'].max()}")
    
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
        raise RuntimeError("All dimensions must exist before loading results") from e
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 4: RESOLVE FKs
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 4] Resolving foreign keys...")
    
    # Create lookup dictionaries
    driver_lookup = dict(zip(dim_driver['driver_ref'], dim_driver['driver_key']))
    constructor_lookup = dict(zip(dim_constructor['constructor_ref'], dim_constructor['constructor_key']))
    
    # Race lookup: composite key (year + round) -> race_key
    dim_race['race_composite'] = dim_race['race_year'].astype(str) + '_' + dim_race['race_round'].astype(str)
    race_lookup = dict(zip(dim_race['race_composite'], dim_race['race_key']))
    
    # Time lookup: race_key -> race_date -> time_key
    race_to_date = dict(zip(dim_race['race_key'], dim_race['race_date']))
    time_map = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))
    
    print(f"Created lookup dictionaries:")
    print(f"  - driver_ref -> driver_key: {len(driver_lookup)} entries")
    print(f"  - constructor_ref -> constructor_key: {len(constructor_lookup)} entries")
    print(f"  - race_year+race_round -> race_key: {len(race_lookup)} entries")
    print(f"  - race_date -> time_key: {len(time_map)} entries")
    
    # Resolve driver_key
    df_api_transformed['driver_key'] = df_api_transformed['driver_ref'].map(driver_lookup)
    unresolved_drivers = df_api_transformed['driver_key'].isna().sum()
    if unresolved_drivers > 0:
        print(f"WARNING: {unresolved_drivers} results have unresolved driver_ref!")
    
    # Resolve constructor_key
    df_api_transformed['constructor_key'] = df_api_transformed['constructor_ref'].map(constructor_lookup)
    unresolved_constructors = df_api_transformed['constructor_key'].isna().sum()
    if unresolved_constructors > 0:
        print(f"WARNING: {unresolved_constructors} results have unresolved constructor_ref!")
    
    # Resolve race_key (composite: year + round)
    df_api_transformed['race_composite'] = (
        df_api_transformed['race_year'].astype(str) + '_' + 
        df_api_transformed['race_round'].astype(str)
    )
    df_api_transformed['race_key'] = df_api_transformed['race_composite'].map(race_lookup)
    unresolved_races = df_api_transformed['race_key'].isna().sum()
    if unresolved_races > 0:
        print(f"WARNING: {unresolved_races} results have unresolved race (year+round)!")
    
    # Resolve time_key (race_key -> race_date -> time_key)
    df_api_transformed['race_date_temp'] = df_api_transformed['race_key'].map(race_to_date)
    df_api_transformed['time_key'] = df_api_transformed['race_date_temp'].map(time_map)
    unresolved_time = df_api_transformed['time_key'].isna().sum()
    if unresolved_time > 0:
        print(f"WARNING: {unresolved_time} results have unresolved time_key!")
    
    # Drop lookup columns
    df_api_transformed = df_api_transformed.drop(columns=[
        'driver_ref', 
        'constructor_ref', 
        'race_year', 
        'race_round', 
        'race_composite', 
        'race_date_temp'
    ])
    
    print(f"All FKs resolved successfully")
    
    # ══════════════════════════════════════════════════════════════
    # STEP 5: LOAD existing Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 5] Loading existing Silver data...")
    
    try:
        # Load all year partitions
        all_existing = []
        for year in range(BACKFILL_YEAR_START, BACKFILL_YEAR_END + 1):
            try:
                year_data = minio.read_parquet(
                    SILVER_BUCKET,
                    f'facts/fact_race_results/year={year}/data.parquet'
                )
                all_existing.append(year_data)
            except Exception:
                # Year partition doesn't exist yet
                continue
        
        if all_existing:
            existing_silver = pd.concat(all_existing, ignore_index=True)
            print(f"Loaded {len(existing_silver):,} existing results from Silver")
            
            # Show source breakdown
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
    # STEP 6: COMPUTE global max result_key across ALL year partitions
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 6] Computing global max result_key across all years...")

    global_max_key = 0
    for year in range(2012, BACKFILL_YEAR_START):
        try:
            year_data = minio.read_parquet(
                SILVER_BUCKET,
                f'facts/fact_race_results/year={year}/data.parquet'
            )
            year_max = int(year_data['result_key'].max())
            if year_max > global_max_key:
                global_max_key = year_max
        except Exception:
            continue

    print(f"Global max result_key from years before {BACKFILL_YEAR_START}: {global_max_key}")

    # ═══════════════════════════════════════════════════════════════
    # STEP 7: MERGE (incremental, idempotent)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 7] Merging API data with existing Silver...")

    merged = merge_results(existing_silver, df_api_transformed, global_max_key)
    
    print(f"Merge complete: {len(merged):,} total results")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 8: WRITE to Silver (partitioned by year)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 8] Writing to Silver layer (partitioned by year)...")
    
    # Ensure correct column order
    column_order = [
        'result_key',
        'result_id',
        'race_key',
        'driver_key',
        'constructor_key',
        'time_key',
        'driver_number',
        'grid',
        'position',
        'position_text',
        'position_order',
        'points',
        'number_of_laps',
        'race_duration_hours',
        'race_duration_milliseconds',
        'fastest_lap',
        'rank',
        'fastest_lap_minutes',
        'fastest_lap_speed',
        'status',
        'year',
        'created_at',
        'source'
    ]
    
    merged = merged[column_order]
    
    # Write partitioned by year
    for year in sorted(merged['year'].unique()):
        year_data = merged[merged['year'] == year]
        object_name = f'facts/fact_race_results/year={year}/data.parquet'
        minio.upload_parquet(year_data, SILVER_BUCKET, object_name)
        print(f"  Year {year}: {len(year_data):,} results written")
    
    print(f"Written to: s3://{SILVER_BUCKET}/facts/fact_race_results/year=*/data.parquet")
    
    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    summary = {
        'existing_count': len(existing_silver),
        'api_scraped': len(df_api_transformed),
        'final_count': len(merged),
        'new_results_added': len(merged) - len(existing_silver) if not existing_silver.empty else len(merged)
    }
    
    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"  Existing results in Silver: {summary['existing_count']:,}")
    print(f"  API results scraped: {summary['api_scraped']}")
    print(f"  Final result count: {summary['final_count']:,}")
    print(f"  New results added: {summary['new_results_added']}")
    
    # Source breakdown
    source_final = merged['source'].value_counts().to_dict()
    print(f"\n  Final source breakdown:")
    for source, count in source_final.items():
        print(f"    {source:15s}: {count:,}")
    
    # result_id ranges
    print(f"\n  result_id ranges:")
    for source in merged['source'].unique():
        source_data = merged[merged['source'] == source]
        print(f"    {source:15s}: {source_data['result_id'].min()} - {source_data['result_id'].max()}")
    
    print("=" * 70)
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='backfill_summary', value=summary)
    
    return summary


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='api_results_bronze_to_silver',
    default_args=default_args,
    description='F1 ETL: API Bronze -> Silver | Backfill race results 2025',
    schedule=None,
    start_date=datetime(2026, 2, 9),
    catchup=False,
    tags=['silver','fact','minio', 'api'],
) as dag:
    
    task_backfill = PythonOperator(
        task_id='backfill_fact_race_results',
        python_callable=backfill_fact_race_results
    )
