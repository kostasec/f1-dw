# dags/api_races_bronze_to_silver.py

"""
F1 Race Backfill Pipeline: Ergast API (2011-2025) -> Silver

Architecture:
- race_key (INT): Surrogate key (Silver internal, sequential 1, 2, 3...)
- race_id (INT): Natural key (CSV: 1-XXX), Pseudo-natural key (API: 10001+)
- race_year + race_round (COMPOSITE): Primary natural key

Merge Logic:
- Match on race_year + race_round (composite natural key)
- CSV has priority (skip API update for existing CSV races)
- Need to lookup circuit_key from circuit_ref

Data Flow:
1. Scrape API (2011-2025)
2. Transform to Silver schema (generate race_id 10001+)
3. Load existing Silver (CSV races)
4. Load dim_circuit for FK lookup
5. Merge (CSV + API) + resolve circuit_key
6. Write to Silver (overwrite)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.s3_helper import MinIOHelper
from scraper.race import RaceScraper

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

def races_equal(row1: pd.Series, row2: pd.Series) -> bool:
    """
    Check if two race rows are identical (for idempotency)
    
    Compares business fields (excluding FK circuit_key which may differ)
    
    Args:
        row1: Existing race row (from Silver)
        row2: New race row (from API)
    
    Returns:
        True if identical, False if any field differs
    """
    compare_fields = [
        'race_name',
        'race_year',
        'race_round',
        'race_date',
        'race_time',
        'fp1_date',
        'fp1_time',
        'fp2_date',
        'fp2_time',
        'fp3_date',
        'fp3_time',
        'qualification_date',
        'qualification_time',
        'sprint_date',
        'sprint_time',
        'race_url'
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


def _should_skip_csv_race(existing_row: pd.Series, race_key_str: str) -> bool:
    """Check if race should be skipped due to CSV priority"""
    if existing_row['source'] == 'f1_data.csv':
        print(f"SKIP: {race_key_str:30s} (CSV priority)")
        return True
    return False


def _should_skip_duplicate(existing_row: pd.Series, new_row: pd.Series, race_key_str: str) -> bool:
    """Check if API race is identical and should be skipped"""
    if races_equal(existing_row, new_row):
        print(f"SKIP: {race_key_str:30s} (duplicate, no change)")
        return True
    return False


def _add_new_race(merged: pd.DataFrame, new_row: pd.Series, 
                  race_key_str: str, max_key: int) -> tuple:
    """Add new API race to merged DataFrame"""
    print(f"ADD: {race_key_str:30s} (new race from API, race_id={new_row['race_id']})")
    
    # Generate NEW surrogate key
    new_key = max_key + 1
    new_row['race_key'] = new_key
    new_row['created_at'] = pd.Timestamp.now()
    
    # Append to merged DataFrame
    merged_updated = pd.concat([merged, new_row.to_frame().T], ignore_index=True)
    return new_key, merged_updated


def _update_existing_race(merged: pd.DataFrame, existing_row: pd.Series, 
                          new_row: pd.Series, race_key_str: str) -> None:
    """Update existing API race with new data"""
    print(f"UPDATE: {race_key_str:30s} (API refresh, data changed)")
    
    # PRESERVE existing metadata
    new_row['race_key'] = existing_row['race_key']
    new_row['race_id'] = existing_row['race_id']
    new_row['source'] = existing_row['source']
    new_row['created_at'] = existing_row['created_at']
    
    # Update row in merged DataFrame
    mask = (merged['race_year'] == new_row['race_year']) & (merged['race_round'] == new_row['race_round'])
    for col in new_row.index:
        merged.loc[mask, col] = new_row[col]


def _process_api_race(merged: pd.DataFrame, new_row: pd.Series, max_key: int) -> tuple:
    """
    Process single API race: skip, update, or add
    
    Returns:
        (updated_max_key, updated_merged_df)
    """
    race_key_str = f"{new_row['race_year']}-R{new_row['race_round']} {new_row['race_name']}"
    
    # Race doesn't exist - ADD new
    mask = (merged['race_year'] == new_row['race_year']) & (merged['race_round'] == new_row['race_round'])
    if not mask.any():
        new_key, merged = _add_new_race(merged, new_row, race_key_str, max_key)
        return new_key, merged
    
    # Race exists - check action
    existing_row = merged[mask].iloc[0]
    
    # CSV has priority - SKIP
    if _should_skip_csv_race(existing_row, race_key_str):
        return max_key, merged
    
    # API race - check if duplicate
    if _should_skip_duplicate(existing_row, new_row, race_key_str):
        return max_key, merged
    
    # Data changed - UPDATE
    _update_existing_race(merged, existing_row, new_row, race_key_str)
    return max_key, merged


def merge_races(existing: pd.DataFrame, new_api: pd.DataFrame) -> pd.DataFrame:
    """
    Merge existing Silver data with new API data (idempotent)
    
    Merge Strategy:
    1. Match on race_year + race_round (COMPOSITE natural key)
    2. CSV has priority (skip API for existing CSV races)
    3. API races update if data changed
    4. New API races are added
    
    Key Generation:
    - race_key (surrogate): Sequential (1, 2, 3, ...)
    - race_id (natural): CSV (1-XXX) or API (10001+)
    - race_year + race_round: PRIMARY composite NK
    
    Args:
        existing: Existing Silver DataFrame (CSV + previous API)
        new_api: New API DataFrame (transformed to Silver schema)
    
    Returns:
        Merged DataFrame (idempotent, deterministic)
    """
    print("\n  Merge Configuration:")
    print(f"Existing races: {len(existing):,}")
    print(f"New API races: {len(new_api)}")
    print(f"Merge key: race_year + race_round (COMPOSITE natural key)")
    print(f"CSV priority: YES (skip API for CSV races)")
    
    # EDGE CASE: First run (no existing data)
    if existing.empty:
        print("\n FIRST RUN: No existing data, adding all API races")
        new_api = new_api.copy()
        new_api['race_key'] = range(1, len(new_api) + 1)
        print(f"Generated race_key: 1-{len(new_api)}")
        print(f"race_id already set: {new_api['race_id'].min()}-{new_api['race_id'].max()}")
        return new_api
    
    # MAIN CASE: Incremental merge
    merged = existing.copy()
    max_race_key = merged['race_key'].max()
    print(f"\n  Current max race_key: {max_race_key}")
    print(f"\n  Processing {len(new_api)} API races:\n")
    
    # Process each API race
    for idx, new_row in new_api.iterrows():
        max_race_key, merged = _process_api_race(merged, new_row, max_race_key)
    
    return merged


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE FUNCTION
# ═══════════════════════════════════════════════════════════════

def backfill_dim_race(**context):
    """
    Backfill dim_race from Ergast API
    
    Pipeline Steps:
    1. Scrape API (2011-2025)
    2. Transform to Silver schema
    3. Load dim_circuit for FK lookup
    4. Resolve circuit_key from circuit_ref
    5. Load existing Silver (CSV + previous API data)
    6. Merge (incremental, idempotent)
    7. Write to Silver
    
    Returns:
        Dict with statistics
    """
    print("=" * 70)
    print("F1 RACE BACKFILL: ERGAST API -> SILVER")
    print("=" * 70)
    
    minio = MinIOHelper()
    scraper = RaceScraper()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: SCRAPE API
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 1] Scraping API...")
    
    df_api_raw = scraper.scrape_races_range(
        year_start=BACKFILL_YEAR_START,
        year_end=BACKFILL_YEAR_END
    )
    
    print(f"API scraping complete: {len(df_api_raw):,} race records")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: TRANSFORM to Silver schema
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 2] Transforming to Silver schema...")
    
    df_api_transformed = scraper.transform_to_silver_schema(df_api_raw)
    
    print(f"Transformation complete: {len(df_api_transformed)} unique races")
    print(f"race_id range: {df_api_transformed['race_id'].min()} - {df_api_transformed['race_id'].max()}")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 3: LOAD dim_circuit for FK lookup
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 3] Loading dim_circuit for FK resolution...")
    
    try:
        dim_circuit = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_circuit/dim_circuit.parquet'
        )
        print(f"Loaded {len(dim_circuit):,} circuits from Silver")
        
        # Create lookup dict: circuit_ref -> circuit_key
        circuit_lookup = dict(zip(dim_circuit['circuit_references'], dim_circuit['circuit_key']))
        print(f"Created circuit_ref -> circuit_key lookup ({len(circuit_lookup)} entries)")
    
    except Exception as e:
        print(f"ERROR: Cannot load dim_circuit - FK resolution impossible!")
        print(f"Details: {e}")
        raise RuntimeError("dim_circuit must exist before loading races") from e
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 4: RESOLVE circuit_key from circuit_ref
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 4] Resolving circuit_key FK...")
    
    df_api_transformed['circuit_key'] = df_api_transformed['circuit_ref'].map(circuit_lookup)
    
    # Check for unresolved FKs
    unresolved = df_api_transformed['circuit_key'].isna().sum()
    if unresolved > 0:
        print(f"WARNING: {unresolved} races have unresolved circuit_ref!")
        print("Unresolved circuit_ref values:")
        print(df_api_transformed[df_api_transformed['circuit_key'].isna()]['circuit_ref'].unique())
    else:
        print(f"All {len(df_api_transformed)} races resolved successfully")
    
    # Drop circuit_ref (no longer needed)
    df_api_transformed = df_api_transformed.drop(columns=['circuit_ref'])
    
    # ══════════════════════════════════════════════════════════════
    # STEP 5: LOAD existing Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 5] Loading existing Silver data...")
    
    try:
        existing_silver = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_race/dim_race.parquet'
        )
        print(f"Loaded {len(existing_silver):,} existing races from Silver")
        
        # Show source breakdown
        if not existing_silver.empty:
            source_counts = existing_silver['source'].value_counts().to_dict()
            print(f"Existing sources: {source_counts}")
            
            # Show race_id ranges
            csv_races = existing_silver[existing_silver['source'] == 'f1_data.csv']
            if not csv_races.empty:
                print(f"CSV race_id range: {csv_races['race_id'].min()} - {csv_races['race_id'].max()}")
    
    except Exception as e:
        print(f"No existing Silver data found (first run)")
        print(f"Details: {e}")
        existing_silver = pd.DataFrame()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 6: MERGE (incremental, idempotent)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 6] Merging API data with existing Silver...")
    
    merged = merge_races(existing_silver, df_api_transformed)
    
    print(f"Merge complete: {len(merged):,} total races")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 7: WRITE to Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 7] Writing to Silver layer...")
    
    # Ensure correct column order
    column_order = [
        'race_key',
        'race_id',
        'circuit_key',
        'race_name',
        'race_year',
        'race_round',
        'race_date',
        'race_time',
        'fp1_date',
        'fp1_time',
        'fp2_date',
        'fp2_time',
        'fp3_date',
        'fp3_time',
        'qualification_date',
        'qualification_time',
        'sprint_date',
        'sprint_time',
        'race_url',
        'created_at',
        'source'
    ]
    
    merged = merged[column_order]
    
    minio.upload_parquet(
        merged,
        SILVER_BUCKET,
        'dimensions/dim_race/dim_race.parquet'
    )
    
    print(f"Written to: s3://{SILVER_BUCKET}/dimensions/dim_race/dim_race.parquet")
    
    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    summary = {
        'existing_count': len(existing_silver),
        'api_scraped': len(df_api_transformed),
        'final_count': len(merged),
        'new_races_added': len(merged) - len(existing_silver) if not existing_silver.empty else len(merged)
    }
    
    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"  Existing races in Silver: {summary['existing_count']:,}")
    print(f"  API races scraped: {summary['api_scraped']}")
    print(f"  Final race count: {summary['final_count']:,}")
    print(f"  New races added: {summary['new_races_added']}")
    
    # Source breakdown
    source_final = merged['source'].value_counts().to_dict()
    print(f"\n  Final source breakdown:")
    for source, count in source_final.items():
        print(f"    {source:15s}: {count:,}")
    
    # race_id ranges
    print(f"\n  race_id ranges:")
    for source in merged['source'].unique():
        source_data = merged[merged['source'] == source]
        print(f"    {source:15s}: {source_data['race_id'].min()} - {source_data['race_id'].max()}")
    
    print("=" * 70)
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='backfill_summary', value=summary)
    
    return summary


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='api_races_bronze_to_silver',
    default_args=default_args,
    description='F1 ETL: API Bronze -> Silver | Backfill races 2025',
    schedule=None,
    start_date=datetime(2026, 2, 9),
    catchup=False,
    tags=['silver','dimension','minio', 'api'],
) as dag:
    
    task_backfill = PythonOperator(
        task_id='backfill_dim_race',
        python_callable=backfill_dim_race
    )
