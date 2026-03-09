# dags/api_driverstandings_bronze_to_silver.py

"""
F1 Driver Standings Backfill Pipeline: Ergast API (2011-2025) -> Silver

Architecture:
- ds_key (INT): Surrogate key (Silver internal, sequential 1, 2, 3...)
- ds_id (INT): Natural key (API: 10000001+)

Merge Logic:
- Match on ds_id (primary natural key)
- API results update if data changed
- New API results are added

Data Flow:
1. Scrape API (2011-2025)
2. Transform to Silver schema (generate ds_id 10000001+)
3. Load dimensions for FK lookup (driver, constructor, race, time)
4. Resolve all FKs (driver_key, constructor_key, race_key, time_key)
5. Load existing Silver (previous API data)
6. Merge (incremental, idempotent)
7. Write to Silver (no partitioning - single file)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.s3_helper import MinIOHelper
from scraper.driverStanding import DriverStandingScraper

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

def dstandings_equal(row1: pd.Series, row2: pd.Series) -> bool:
    """
    Check if two driver standing rows are identical (for idempotency)
    
    Compares business fields
    
    Args:
        row1: Existing standing row (from Silver)
        row2: New standing row (from API)
    
    Returns:
        True if identical, False if any field differs
    """
    compare_fields = [
        'ds_position',
        'ds_points',
        'ds_wins'
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

def _should_skip_csv_result(existing_row: pd.Series, ds_id: int) -> bool:
    """Check if result should be skipped due to CSV priority"""
    source = str(existing_row['source']).lower()
    if 'f1_data.csv' in source:
        print(f"SKIP: ds_id={ds_id:8d} (CSV priority)")
        return True
    return False

def _should_skip_duplicate(existing_row: pd.Series, new_row: pd.Series, ds_id: int) -> bool:
    """Check if API standing is identical and should be skipped"""
    if dstandings_equal(existing_row, new_row):
        print(f"SKIP: ds_id={ds_id:8d} (duplicate, no change)")
        return True
    return False


def _add_new_standing(merged: pd.DataFrame, new_row: pd.Series, max_key: int) -> tuple:
    """Add new API standing to merged DataFrame"""
    ds_id = new_row['ds_id']
    print(f"ADD: ds_id={ds_id:8d} (new standing from API)")
    
    # Generate NEW surrogate key
    new_key = max_key + 1
    new_row['ds_key'] = new_key
    new_row['created_at'] = pd.Timestamp.now()
    
    # Append to merged DataFrame
    merged_updated = pd.concat([merged, new_row.to_frame().T], ignore_index=True)
    return new_key, merged_updated


def _update_existing_standing(merged: pd.DataFrame, existing_row: pd.Series, 
                             new_row: pd.Series) -> None:
    """Update existing API standing with new data"""
    ds_id = new_row['ds_id']
    print(f"UPDATE: ds_id={ds_id:8d} (API refresh, data changed)")
    
    # PRESERVE existing metadata
    new_row['ds_key'] = existing_row['ds_key']
    new_row['ds_id'] = existing_row['ds_id']
    new_row['source'] = existing_row['source']
    new_row['created_at'] = existing_row['created_at']
    
    # Update row in merged DataFrame
    mask = merged['ds_id'] == ds_id
    for col in new_row.index:
        merged.loc[mask, col] = new_row[col]


def _process_api_standing(merged: pd.DataFrame, new_row: pd.Series, max_key: int) -> tuple:
    """
    Process single API standing: skip, update, or add
    
    Returns:
        (updated_max_key, updated_merged_df)
    """
    ds_id = new_row['ds_id']
    
    # Standing doesn't exist - ADD new
    if ds_id not in merged['ds_id'].values:
        new_key, merged = _add_new_standing(merged, new_row, max_key)
        return new_key, merged
    
    # Standing exists - check if duplicate
    existing_row = merged[merged['ds_id'] == ds_id].iloc[0]
    
    # CSV has priority - SKIP
    if _should_skip_csv_result(existing_row, ds_id):
        return max_key, merged

    # API result - check if duplicate
    if _should_skip_duplicate(existing_row, new_row, ds_id):
        return max_key, merged
    
    # Data changed - UPDATE
    _update_existing_standing(merged, existing_row, new_row)
    return max_key, merged


def merge_dstandings(existing: pd.DataFrame, new_api: pd.DataFrame) -> pd.DataFrame:
    """
    Merge existing Silver data with new API data (idempotent)
    
    Merge Strategy:
    1. Match on ds_id (PRIMARY natural key)
    2. API results update if data changed
    3. New API results are added
    
    Key Generation:
    - ds_key (surrogate): Sequential (1, 2, 3, ...)
    - ds_id (natural): API (10000001+)
    
    Args:
        existing: Existing Silver DataFrame (previous API)
        new_api: New API DataFrame (transformed to Silver schema)
    
    Returns:
        Merged DataFrame (idempotent, deterministic)
    """
    print("\n  Merge Configuration:")
    print(f"Existing standings: {len(existing):,}")
    print(f"New API standings: {len(new_api)}")
    print(f"Merge key: ds_id (PRIMARY natural key)")
    
    # EDGE CASE: First run (no existing data)
    if existing.empty:
        print("\n FIRST RUN: No existing data, adding all API standings")
        new_api = new_api.copy()
        new_api['ds_key'] = range(1, len(new_api) + 1)
        print(f"Generated ds_key: 1-{len(new_api)}")
        print(f"ds_id already set: {new_api['ds_id'].min()}-{new_api['ds_id'].max()}")
        return new_api
    
    # MAIN CASE: Incremental merge
    merged = existing.copy()
    max_ds_key = merged['ds_key'].max()
    print(f"\n  Current max ds_key: {max_ds_key}")
    print(f"\n  Processing {len(new_api)} API standings:\n")
    
    # Process each API standing
    for idx, new_row in new_api.iterrows():
        max_ds_key, merged = _process_api_standing(merged, new_row, max_ds_key)
    
    # Sort by ds_key to ensure sequential order
    merged = merged.sort_values('ds_key').reset_index(drop=True)
    
    return merged


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE FUNCTION
# ═══════════════════════════════════════════════════════════════

def backfill_fact_driver_standings(**context):
    """
    Backfill fact_driver_standings from Ergast API
    
    Pipeline Steps:
    1. Scrape API (2011-2025)
    2. Transform to Silver schema
    3. Load dimensions for FK lookup
    4. Resolve all FKs (driver_key, constructor_key, race_key, time_key)
    5. Load existing Silver (previous API data)
    6. Merge (incremental, idempotent)
    7. Write to Silver
    
    Returns:
        Dict with statistics
    """
    print("=" * 70)
    print("F1 DRIVER STANDINGS BACKFILL: ERGAST API -> SILVER")
    print("=" * 70)
    
    minio = MinIOHelper()
    scraper = DriverStandingScraper()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: SCRAPE API
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 1] Scraping API...")
    
    df_api_raw = scraper.scrape_dstandings_range(
        year_start=BACKFILL_YEAR_START,
        year_end=BACKFILL_YEAR_END
    )
    
    print(f"API scraping complete: {len(df_api_raw):,} standing records")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: TRANSFORM to Silver schema
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 2] Transforming to Silver schema...")
    
    df_api_transformed = scraper.transform_to_silver_schema(df_api_raw)
    
    print(f"Transformation complete: {len(df_api_transformed)} standings")
    print(f"ds_id range: {df_api_transformed['ds_id'].min()} - {df_api_transformed['ds_id'].max()}")
    
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
        raise RuntimeError("All dimensions must exist before loading driver standings") from e
    
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
        print(f"WARNING: {unresolved_drivers} standings have unresolved driver_ref!")
    
    # Resolve constructor_key
    df_api_transformed['constructor_key'] = df_api_transformed['constructor_ref'].map(constructor_lookup)
    unresolved_constructors = df_api_transformed['constructor_key'].isna().sum()
    if unresolved_constructors > 0:
        print(f"WARNING: {unresolved_constructors} standings have unresolved constructor_ref!")
    
    # Resolve race_key (composite: year + round)
    df_api_transformed['race_composite'] = (
        df_api_transformed['race_year'].astype(str) + '_' + 
        df_api_transformed['race_round'].astype(str)
    )
    df_api_transformed['race_key'] = df_api_transformed['race_composite'].map(race_lookup)
    unresolved_races = df_api_transformed['race_key'].isna().sum()
    if unresolved_races > 0:
        print(f"WARNING: {unresolved_races} standings have unresolved race (year+round)!")
    
    # Resolve time_key (race_key -> race_date -> time_key)
    df_api_transformed['race_date_temp'] = df_api_transformed['race_key'].map(race_to_date)
    df_api_transformed['time_key'] = df_api_transformed['race_date_temp'].map(time_map)
    unresolved_time = df_api_transformed['time_key'].isna().sum()
    if unresolved_time > 0:
        print(f"WARNING: {unresolved_time} standings have unresolved time_key!")
    
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
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 5: LOAD existing Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 5] Loading existing Silver data...")
    
    try:
        existing_silver = minio.read_parquet(
            SILVER_BUCKET,
            'facts/fact_driver_standings/fact_driver_standings.parquet'
        )
        print(f"Loaded {len(existing_silver):,} existing standings from Silver")
        
        # Show source breakdown
        source_counts = existing_silver['source'].value_counts().to_dict()
        print(f"Existing sources: {source_counts}")
    
    except Exception as e:
        print(f"No existing Silver data found (first run)")
        print(f"Details: {e}")
        existing_silver = pd.DataFrame()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 6: MERGE (incremental, idempotent)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 6] Merging API data with existing Silver...")
    
    merged = merge_dstandings(existing_silver, df_api_transformed)
    
    print(f"Merge complete: {len(merged):,} total standings")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 7: WRITE to Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 7] Writing to Silver layer...")
    
    # Ensure correct column order
    column_order = [
        'ds_key',
        'ds_id',
        'race_key',
        'driver_key',
        'constructor_key',
        'time_key',
        'ds_position',
        'ds_points',
        'ds_wins',
        'created_at',
        'source'
    ]
    
    merged = merged[column_order]
    
    # Write to Silver (single file, not partitioned)
    minio.upload_parquet(
        merged, 
        SILVER_BUCKET, 
        'facts/fact_driver_standings/fact_driver_standings.parquet'
    )
    
    print(f"Written to: s3://{SILVER_BUCKET}/facts/fact_driver_standings/fact_driver_standings.parquet")
    
    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    summary = {
        'existing_count': len(existing_silver),
        'api_scraped': len(df_api_transformed),
        'final_count': len(merged),
        'new_standings_added': len(merged) - len(existing_silver) if not existing_silver.empty else len(merged)
    }
    
    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"  Existing standings in Silver: {summary['existing_count']:,}")
    print(f"  API standings scraped: {summary['api_scraped']}")
    print(f"  Final standing count: {summary['final_count']:,}")
    print(f"  New standings added: {summary['new_standings_added']}")
    
    # Source breakdown
    source_final = merged['source'].value_counts().to_dict()
    print(f"\n  Final source breakdown:")
    for source, count in source_final.items():
        print(f"    {source:15s}: {count:,}")
    
    # ds_id ranges
    print(f"\n  ds_id ranges:")
    for source in merged['source'].unique():
        source_data = merged[merged['source'] == source]
        print(f"    {source:15s}: {source_data['ds_id'].min()} - {source_data['ds_id'].max()}")
    
    print("=" * 70)
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='backfill_summary', value=summary)
    
    return summary


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='api_driverstandings_bronze_to_silver',
    default_args=default_args,
    description='F1 ETL: API Bronze -> Silver | Backfill driver standings 2025',
    schedule=None,
    start_date=datetime(2026, 2, 9),
    catchup=False,
    tags=['silver','fact','minio', 'api'],
) as dag:
    
    task_backfill = PythonOperator(
        task_id='backfill_fact_driver_standings',
        python_callable=backfill_fact_driver_standings
    )