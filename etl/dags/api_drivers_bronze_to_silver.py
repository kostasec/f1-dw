"""
F1 Driver Backfill Pipeline: Ergast API (2011-2025) → Silver

Architecture:
- driver_key (INT): Surrogate key (Silver interno, sequential 1, 2, 3...)
- driver_id (INT): Natural key (CSV: 1-857), Pseudo-natural key (API: 10001+)
- driver_ref (STRING): Natural key (universal, "hamilton")

Merge Logic:
- Match on driver_ref (primary natural key)
- CSV has priority (skip API update for existing CSV drivers)

Data Flow:
1. Scrape API (2011-2025)
2. Transform to Silver schema (generate driver_id 10001+)
3. Load existing Silver (CSV 857 drivers)
4. Merge (CSV + API)
5. Write to Silver (overwrite)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.s3_helper import MinIOHelper
from scraper.driver import DriverScraper

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

def drivers_equal(row1: pd.Series, row2: pd.Series) -> bool:
    """
    Check if two driver rows are identical (for idempotency)
    
    Compares business fields:
    - driver_forename, driver_surname
    - driver_dob, driver_nationality
    - driver_number, driver_code
    
    Args:
        row1: Existing driver row (from Silver)
        row2: New driver row (from API)
    
    Returns:
        True if identical, False if any field differs
    """
    compare_fields = [
        'driver_forename',
        'driver_surname',
        'driver_dob',
        'driver_nationality',
        'driver_number',
        'driver_code'
    ]
    
    for field in compare_fields:
        val1, val2 = row1.get(field), row2.get(field)
        
        # Both NULL → equal, continue
        if pd.isna(val1) and pd.isna(val2):
            continue
        
        # One is NULL, other is not → different
        if pd.isna(val1) or pd.isna(val2):
            return False
        
        # Both have values → compare
        if val1 != val2:
            return False
    
    return True


def _should_skip_csv_driver(existing_row: pd.Series, driver_ref: str) -> bool:
    """Check if driver should be skipped due to CSV priority"""
    if existing_row['source'] == 'f1_data.csv':
        print(f"SKIP: {driver_ref:20s} (CSV priority)")
        return True
    return False


def _should_skip_duplicate_driver(existing_row: pd.Series, new_row: pd.Series, driver_ref: str) -> bool:
    """Check if API driver is identical and should be skipped"""
    if drivers_equal(existing_row, new_row):
        print(f"SKIP: {driver_ref:20s} (duplicate, no change)")
        return True
    return False


def _update_existing_driver(merged: pd.DataFrame, existing_row: pd.Series, 
                            new_row: pd.Series, driver_ref: str) -> None:
    """Update existing API driver with new data"""
    print(f"UPDATE: {driver_ref:20s} (API refresh, data changed)")
    
    # PRESERVE existing metadata
    new_row['driver_key'] = existing_row['driver_key']
    new_row['driver_id'] = existing_row['driver_id']
    new_row['source'] = existing_row['source']
    new_row['created_at'] = existing_row['created_at']
    
    # Update row in merged DataFrame - use column-by-column assignment to preserve dtypes
    mask = merged['driver_ref'] == driver_ref
    for col in new_row.index:
        merged.loc[mask, col] = new_row[col]


def _add_new_driver(merged: pd.DataFrame, new_row: pd.Series, 
                    driver_ref: str, max_key: int) -> int:
    """Add new API driver to merged DataFrame"""
    print(f"ADD: {driver_ref:20s} (new driver from API, driver_id={new_row['driver_id']})")
    
    # Generate NEW surrogate key
    new_key = max_key + 1
    new_row['driver_key'] = new_key
    new_row['created_at'] = pd.Timestamp.now()
    
    # Append to merged DataFrame (in-place via parent scope)
    merged_updated = pd.concat([merged, new_row.to_frame().T], ignore_index=True)
    return new_key, merged_updated


def _process_api_driver(merged: pd.DataFrame, new_row: pd.Series, max_key: int) -> tuple:
    """
    Process single API driver: skip, update, or add
    
    Returns:
        (updated_max_key, updated_merged_df)
    """
    driver_ref = new_row['driver_ref']
    
    # Driver doesn't exist - ADD new
    if driver_ref not in merged['driver_ref'].values:
        new_key, merged = _add_new_driver(merged, new_row, driver_ref, max_key)
        return new_key, merged
    
    # Driver exists - check action
    existing_row = merged[merged['driver_ref'] == driver_ref].iloc[0]
    
    # CSV has priority - SKIP
    if _should_skip_csv_driver(existing_row, driver_ref):
        return max_key, merged
    
    # API driver - check if duplicate
    if _should_skip_duplicate_driver(existing_row, new_row, driver_ref):
        return max_key, merged
    
    # Data changed - UPDATE
    _update_existing_driver(merged, existing_row, new_row, driver_ref)
    return max_key, merged


def merge_drivers(existing: pd.DataFrame, new_api: pd.DataFrame) -> pd.DataFrame:
    """
    Merge existing Silver data with new API data (idempotent)
    
    Merge Strategy:
    1. Match on driver_ref (PRIMARY natural key)
    2. CSV has priority (skip API for existing CSV drivers)
    3. API drivers update if data changed
    4. New API drivers are added
    
    Key Generation:
    - driver_key (surrogate): Sequential (1, 2, 3, ...)
    - driver_id (natural): CSV (1-857) or API (10001+)
    - driver_ref (natural): Primary NK ("hamilton", "verstappen")
    
    Args:
        existing: Existing Silver DataFrame (CSV + previous API)
        new_api: New API DataFrame (transformed to Silver schema)
    
    Returns:
        Merged DataFrame (idempotent, deterministic)
    """
    print("\n  Merge Configuration:")
    print(f"Existing drivers: {len(existing):,}")
    print(f"New API drivers: {len(new_api)}")
    print(f"Merge key: driver_ref (PRIMARY natural key)")
    print(f"CSV priority: YES (skip API for CSV drivers)")
    
    # EDGE CASE: First run (no existing data)
    if existing.empty:
        print("\n FIRST RUN: No existing data, adding all API drivers")
        new_api = new_api.copy()
        new_api['driver_key'] = range(1, len(new_api) + 1)
        print(f"Generated driver_key: 1-{len(new_api)}")
        print(f"driver_id already set: {new_api['driver_id'].min()}-{new_api['driver_id'].max()}")
        return new_api
    
    # MAIN CASE: Incremental merge
    merged = existing.copy()
    max_driver_key = merged['driver_key'].max()
    print(f"\n  Current max driver_key: {max_driver_key}")
    print(f"\n  Processing {len(new_api)} API drivers:\n")
    
    # Process each API driver
    for idx, new_row in new_api.iterrows():
        max_driver_key, merged = _process_api_driver(merged, new_row, max_driver_key)
    
    return merged


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE FUNCTION
# ═══════════════════════════════════════════════════════════════

def backfill_dim_driver(**context):
    """
    Backfill dim_driver from Ergast API
    
    Pipeline Steps:
    1. Scrape API (2011-2025)
    2. Transform to Silver schema
    3. Load existing Silver (CSV + previous API data)
    4. Merge (incremental, idempotent)
    5. Write to Silver
    
    Returns:
        Dict with statistics
    """
    print("=" * 70)
    print("F1 DRIVER BACKFILL: ERGAST API → SILVER")
    print("=" * 70)
    
    minio = MinIOHelper()
    scraper = DriverScraper()

    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: SCRAPE API
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 1] Scraping API...")
    
    df_api_raw = scraper.scrape_drivers_range(
        year_start=BACKFILL_YEAR_START,
        year_end=BACKFILL_YEAR_END
    )
    
    print(f"API scraping complete: {len(df_api_raw):,} driver records")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: TRANSFORM to Silver schema
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 2] Transforming to Silver schema...")
    
    df_api_transformed = scraper.transform_to_silver_schema(df_api_raw)
    
    print(f"Transformation complete: {len(df_api_transformed)} unique drivers")
    print(f"driver_id range: {df_api_transformed['driver_id'].min()} - {df_api_transformed['driver_id'].max()}")
    
    # ══════════════════════════════════════════════════════════════
    # STEP 3: LOAD existing Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 3] Loading existing Silver data...")
    
    try:
        existing_silver = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_driver/dim_driver.parquet'
        )
        print(f"Loaded {len(existing_silver):,} existing drivers from Silver")
        
        # Show source breakdown
        if not existing_silver.empty:
            source_counts = existing_silver['source'].value_counts().to_dict()
            print(f"Existing sources: {source_counts}")
            
            # Show driver_id ranges
            csv_drivers = existing_silver[existing_silver['source'] == 'f1_data.csv']
            if not csv_drivers.empty:
                print(f"CSV driver_id range: {csv_drivers['driver_id'].min()} - {csv_drivers['driver_id'].max()}")
    
    except Exception as e:
        print(f"No existing Silver data found (first run)")
        print(f"Details: {e}")
        existing_silver = pd.DataFrame()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 4: MERGE (incremental, idempotent)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 4] Merging API data with existing Silver...")
    
    merged = merge_drivers(existing_silver, df_api_transformed)
    
    print(f"Merge complete: {len(merged):,} total drivers")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 5: WRITE to Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 5] Writing to Silver layer...")
    
    # Ensure correct column order
    column_order = [
        'driver_key',
        'driver_id',
        'driver_ref',
        'driver_number',
        'driver_code',
        'driver_forename',
        'driver_surname',
        'driver_dob',
        'driver_nationality',
        'driver_url',
        'created_at',
        'source'
    ]
    
    merged = merged[column_order]
    
    minio.upload_parquet(
        merged,
        SILVER_BUCKET,
        'dimensions/dim_driver/dim_driver.parquet'
    )
    
    print(f"Written to: s3://{SILVER_BUCKET}/dimensions/dim_driver/dim_driver.parquet")
    
    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    summary = {
        'existing_count': len(existing_silver),
        'api_scraped': len(df_api_transformed),
        'final_count': len(merged),
        'new_drivers_added': len(merged) - len(existing_silver) if not existing_silver.empty else len(merged)
    }
    
    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"  Existing drivers in Silver: {summary['existing_count']:,}")
    print(f"  API drivers scraped: {summary['api_scraped']}")
    print(f"  Final driver count: {summary['final_count']:,}")
    print(f"  New drivers added: {summary['new_drivers_added']}")
    
    # Source breakdown
    source_final = merged['source'].value_counts().to_dict()
    print(f"\n  Final source breakdown:")
    for source, count in source_final.items():
        print(f"    {source:15s}: {count:,}")
    
    # driver_id ranges
    print(f"\n  driver_id ranges:")
    for source in merged['source'].unique():
        source_data = merged[merged['source'] == source]
        print(f"    {source:15s}: {source_data['driver_id'].min()} - {source_data['driver_id'].max()}")
    
    print("=" * 70)
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='backfill_summary', value=summary)
    
    return summary


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='api_drivers_bronze_to_silver',
    default_args=default_args,
    description='F1 ETL: API Bronze → Silver | Backfill drivers 2025',
    schedule=None,
    start_date=datetime(2026, 2, 6),
    catchup=False,
    tags=['silver','dimension','minio', 'api'],
) as dag:
    
    task_backfill = PythonOperator(
        task_id='backfill_dim_driver',
        python_callable=backfill_dim_driver
    )
