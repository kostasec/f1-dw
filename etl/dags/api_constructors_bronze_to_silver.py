# dags/f1_etl_api_constructors_bronze_to_silver.py

"""
F1 Constructor Backfill Pipeline: Ergast API (2011-2025) → Silver

Architecture:
- constructor_key (INT): Surrogate key (Silver interno, sequential 1, 2, 3...)
- constructor_id (INT): Natural key (CSV: 1-XXX), Pseudo-natural key (API: 10001+)
- constructor_ref (STRING): Natural key (universal, "ferrari", "mercedes")

Merge Logic:
- Match on constructor_ref (primary natural key)
- CSV has priority (skip API update for existing CSV constructors)

Data Flow:
1. Scrape API (2011-2025)
2. Transform to Silver schema (generate constructor_id 10001+)
3. Load existing Silver (CSV constructors)
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
from scraper.constructor import ConstructorScraper
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

def constructors_equal(row1: pd.Series, row2: pd.Series) -> bool:
    """
    Check if two constructor rows are identical (for idempotency)
    
    Compares business fields:
    - constructor_name
    - constructor_nationality
    - constructor_url
    
    Args:
        row1: Existing constructor row (from Silver)
        row2: New constructor row (from API)
    
    Returns:
        True if identical, False if any field differs
    """
    compare_fields = [
        'constructor_name',
        'constructor_nationality',
        'constructor_url'
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

def _should_skip_csv_constructor(existing_row: pd.Series, constructor_ref: str) -> bool:
    """Check if constructor should be skipped due to CSV priority"""
    if existing_row['source'] == 'f1_data.csv':
        print(f"SKIP: {constructor_ref:20s} (CSV priority)")
        return True
    return False


def _should_skip_duplicate(existing_row: pd.Series, new_row: pd.Series, constructor_ref: str) -> bool:
    """Check if API constructor is identical and should be skipped"""
    if constructors_equal(existing_row, new_row):
        print(f"SKIP: {constructor_ref:20s} (duplicate, no change)")
        return True
    return False


def _update_existing_constructor(merged: pd.DataFrame, existing_row: pd.Series, 
                                  new_row: pd.Series, constructor_ref: str) -> None:
    """Update existing API constructor with new data"""
    print(f"UPDATE: {constructor_ref:20s} (API refresh, data changed)")
    
    # PRESERVE existing metadata
    new_row['constructor_key'] = existing_row['constructor_key']
    new_row['constructor_id'] = existing_row['constructor_id']
    new_row['source'] = existing_row['source']
    new_row['created_at'] = existing_row['created_at']
    
    # Update row in merged DataFrame - use column-by-column assignment to preserve dtypes
    mask = merged['constructor_ref'] == constructor_ref
    for col in new_row.index:
        merged.loc[mask, col] = new_row[col]


def _add_new_constructor(merged: pd.DataFrame, new_row: pd.Series, 
                         constructor_ref: str, max_key: int) -> int:
    """Add new API constructor to merged DataFrame"""
    print(f"ADD: {constructor_ref:20s} (new constructor from API, constructor_id={new_row['constructor_id']})")
    
    # Generate NEW surrogate key
    new_key = max_key + 1
    new_row['constructor_key'] = new_key
    new_row['created_at'] = pd.Timestamp.now()
    
    # Append to merged DataFrame (in-place via parent scope)
    merged_updated = pd.concat([merged, new_row.to_frame().T], ignore_index=True)
    return new_key, merged_updated


def _process_api_constructor(merged: pd.DataFrame, new_row: pd.Series, max_key: int) -> tuple:
    """
    Process single API constructor: skip, update, or add
    
    Returns:
        (updated_max_key, updated_merged_df)
    """
    constructor_ref = new_row['constructor_ref']
    
    # Constructor doesn't exist - ADD new
    if constructor_ref not in merged['constructor_ref'].values:
        new_key, merged = _add_new_constructor(merged, new_row, constructor_ref, max_key)
        return new_key, merged
    
    # Constructor exists - check action
    existing_row = merged[merged['constructor_ref'] == constructor_ref].iloc[0]
    
    # CSV has priority - SKIP
    if _should_skip_csv_constructor(existing_row, constructor_ref):
        return max_key, merged
    
    # API constructor - check if duplicate
    if _should_skip_duplicate(existing_row, new_row, constructor_ref):
        return max_key, merged
    
    # Data changed - UPDATE
    _update_existing_constructor(merged, existing_row, new_row, constructor_ref)
    return max_key, merged


def merge_constructors(existing: pd.DataFrame, new_api: pd.DataFrame) -> pd.DataFrame:
    """
    Merge existing Silver data with new API data (idempotent)
    
    Merge Strategy:
    1. Match on constructor_ref (PRIMARY natural key)
    2. CSV has priority (skip API for existing CSV constructors)
    3. API constructors update if data changed
    4. New API constructors are added
    
    Key Generation:
    - constructor_key (surrogate): Sequential (1, 2, 3, ...)
    - constructor_id (natural): CSV (1-XXX) or API (10001+)
    - constructor_ref (natural): Primary NK ("ferrari", "mercedes")
    
    Args:
        existing: Existing Silver DataFrame (CSV + previous API)
        new_api: New API DataFrame (transformed to Silver schema)
    
    Returns:
        Merged DataFrame (idempotent, deterministic)
    """
    print("\n  Merge Configuration:")
    print(f"Existing constructors: {len(existing):,}")
    print(f"New API constructors: {len(new_api)}")
    print(f"Merge key: constructor_ref (PRIMARY natural key)")
    print(f"CSV priority: YES (skip API for CSV constructors)")
    
    # EDGE CASE: First run (no existing data)
    if existing.empty:
        print("\n FIRST RUN: No existing data, adding all API constructors")
        new_api = new_api.copy()
        new_api['constructor_key'] = range(1, len(new_api) + 1)
        print(f"Generated constructor_key: 1-{len(new_api)}")
        print(f"constructor_id already set: {new_api['constructor_id'].min()}-{new_api['constructor_id'].max()}")
        return new_api
    
    # MAIN CASE: Incremental merge
    merged = existing.copy()
    max_constructor_key = merged['constructor_key'].max()
    print(f"\n  Current max constructor_key: {max_constructor_key}")
    print(f"\n  Processing {len(new_api)} API constructors:\n")
    
    # Process each API constructor
    for idx, new_row in new_api.iterrows():
        max_constructor_key, merged = _process_api_constructor(merged, new_row, max_constructor_key)
    
    return merged


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE FUNCTION
# ═══════════════════════════════════════════════════════════════

def backfill_dim_constructor(**context):
    """
    Backfill dim_constructor from Ergast API
    
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
    print("F1 CONSTRUCTOR BACKFILL: ERGAST API → SILVER")
    print("=" * 70)
    
    minio = MinIOHelper()
    scraper = ConstructorScraper()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: SCRAPE API
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 1] Scraping API...")
    
    df_api_raw = scraper.scrape_constructors_range(
        year_start=BACKFILL_YEAR_START,
        year_end=BACKFILL_YEAR_END
    )
    
    print(f"API scraping complete: {len(df_api_raw):,} constructor records")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: TRANSFORM to Silver schema
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 2] Transforming to Silver schema...")
    
    df_api_transformed = scraper.transform_to_silver_schema(df_api_raw)
    
    print(f"Transformation complete: {len(df_api_transformed)} unique constructors")
    print(f"constructor_id range: {df_api_transformed['constructor_id'].min()} - {df_api_transformed['constructor_id'].max()}")
    
    # ══════════════════════════════════════════════════════════════
    # STEP 3: LOAD existing Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 3] Loading existing Silver data...")
    
    try:
        existing_silver = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_constructor/dim_constructor.parquet'
        )
        print(f"Loaded {len(existing_silver):,} existing constructors from Silver")
        
        # Show source breakdown
        if not existing_silver.empty:
            source_counts = existing_silver['source'].value_counts().to_dict()
            print(f"Existing sources: {source_counts}")
            
            # Show constructor_id ranges
            csv_constructors = existing_silver[existing_silver['source'] == 'f1_data.csv']
            if not csv_constructors.empty:
                print(f"CSV constructor_id range: {csv_constructors['constructor_id'].min()} - {csv_constructors['constructor_id'].max()}")
    
    except Exception as e:
        print(f"No existing Silver data found (first run)")
        print(f"Details: {e}")
        existing_silver = pd.DataFrame()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 4: MERGE (incremental, idempotent)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 4] Merging API data with existing Silver...")
    
    merged = merge_constructors(existing_silver, df_api_transformed)
    
    print(f"Merge complete: {len(merged):,} total constructors")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 5: WRITE to Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 5] Writing to Silver layer...")
    
    # Ensure correct column order
    column_order = [
        'constructor_key',
        'constructor_id',
        'constructor_ref',
        'constructor_name',
        'constructor_nationality',
        'constructor_url',
        'created_at',
        'source'
    ]
    
    merged = merged[column_order]
    
    minio.upload_parquet(
        merged,
        SILVER_BUCKET,
        'dimensions/dim_constructor/dim_constructor.parquet'
    )
    
    print(f"Written to: s3://{SILVER_BUCKET}/dimensions/dim_constructor/dim_constructor.parquet")
    
    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    summary = {
        'existing_count': len(existing_silver),
        'api_scraped': len(df_api_transformed),
        'final_count': len(merged),
        'new_constructors_added': len(merged) - len(existing_silver) if not existing_silver.empty else len(merged)
    }
    
    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"  Existing constructors in Silver: {summary['existing_count']:,}")
    print(f"  API constructors scraped: {summary['api_scraped']}")
    print(f"  Final constructor count: {summary['final_count']:,}")
    print(f"  New constructors added: {summary['new_constructors_added']}")
    
    # Source breakdown
    source_final = merged['source'].value_counts().to_dict()
    print(f"\n  Final source breakdown:")
    for source, count in source_final.items():
        print(f"    {source:15s}: {count:,}")
    
    # constructor_id ranges
    print(f"\n  constructor_id ranges:")
    for source in merged['source'].unique():
        source_data = merged[merged['source'] == source]
        print(f"    {source:15s}: {source_data['constructor_id'].min()} - {source_data['constructor_id'].max()}")
    
    print("=" * 70)
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='backfill_summary', value=summary)
    
    return summary


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='api_constructors_bronze_to_silver',
    default_args=default_args,
    description='F1 ETL: API Bronze → Silver | Backfill constructors 2025',
    schedule=None,
    start_date=datetime(2026, 2, 7),
    catchup=False,
    tags=['silver','dimension','minio', 'api'],
) as dag:
    
    task_backfill = PythonOperator(
        task_id='backfill_dim_constructor',
        python_callable=backfill_dim_constructor
    )
