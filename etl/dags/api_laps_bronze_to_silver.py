# dags/api_laps_bronze_to_silver.py

"""
F1 Lap Times Backfill Pipeline: Ergast API (2011-2025) -> Silver

Architecture:
- lap_key (INT): Surrogate key (Silver internal, sequential 1, 2, 3...)
- NO natural key (composite: race + driver + lap, but not explicit ID)

Merge Logic:
- Match on: race_year + race_round + driver_ref + lap
- CSV has priority (skip API update for existing CSV laps)
- Need FK lookup: driver_ref, race_year+race_round

Data Flow:
1. Scrape API (2011-2025, all rounds, all laps) - MASSIVE DATA VOLUME
2. Transform to Silver schema
3. Load dimensions for FK lookup (driver, race, constructor, time)
4. Resolve FKs (driver_key, race_key, constructor_key, time_key)
5. Load existing Silver (CSV laps)
6. Merge (CSV + API)
7. Write to Silver (partitioned by year)

WARNING: This is a MASSIVE dataset:
- Each race: ~50-70 laps × 20 drivers = 1000-1400 records
- 2011-2025: ~15 years × 20 races × 1200 records = ~360,000 lap times
- API rate limit: 20s delay → ~300 races × 20s = 6000s = 100 minutes minimum
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.s3_helper import MinIOHelper
from scraper.lap import LapScraper

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

def laps_equal(row1: pd.Series, row2: pd.Series) -> bool:
    """
    Check if two lap rows are identical (for idempotency)
    
    Compares business fields (excluding FKs which may differ)
    
    Args:
        row1: Existing lap row (from Silver)
        row2: New lap row (from API)
    
    Returns:
        True if identical, False if any field differs
    """
    compare_fields = [
        'lap',
        'lap_position',
        'lap_minutes',
        'lap_milliseconds',
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


def merge_laps(existing: pd.DataFrame, api_data: pd.DataFrame, 
               driver_map: dict, race_map: dict, constructor_map: dict,
               race_to_constructor: dict, race_to_date: dict, time_map: dict,
               race_key_to_year_round: dict) -> pd.DataFrame:
    """
    Merge existing Silver laps with new API laps
    
    Merge Strategy:
    - CSV data has priority (NEVER overwrite CSV laps)
    - API data fills gaps (only add new laps)
    - Match on: race_year + race_round + driver_ref + lap (composite natural key)
    - Preserve: lap_key (existing), source='f1_data.csv' (CSV records)
    
    Args:
        existing: Existing laps from Silver (may be empty if first run)
        api_data: New laps from API (after transformation)
        driver_map: {driver_ref: driver_key} mapping
        race_map: {(race_year, race_round): race_key} mapping
        constructor_map: {constructor_ref: constructor_key} mapping
        race_to_constructor: {(race_year, race_round, driver_ref): constructor_ref} mapping
        race_to_date: {race_key: race_date} mapping
        time_map: {race_date: time_key} mapping
        race_key_to_year_round: {race_key: (race_year, race_round)} reverse mapping
    
    Returns:
        Merged DataFrame (existing + new API laps)
    """
    print("\n" + "="*70)
    print("MERGING LAPS: CSV + API")
    print("="*70)
    
    if existing.empty:
        print("No existing data - this is the first backfill")
        new_laps = api_data.copy()
    else:
        print(f"Existing laps: {len(existing):,} (from CSV and previous API runs)")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 1: Identify CSV laps (MUST preserve these)
        # ═══════════════════════════════════════════════════════════
        csv_laps = existing[existing['source'] == 'f1_data.csv'].copy()
        api_laps_existing = existing[existing['source'] == 'api'].copy()
        
        print(f"  CSV laps (preserve): {len(csv_laps):,}")
        print(f"  API laps (existing): {len(api_laps_existing):,}")
        
        # Reconstruct race_year and race_round from race_key (needed for matching)
        if not csv_laps.empty and 'race_key' in csv_laps.columns:
            csv_laps[['race_year', 'race_round']] = csv_laps['race_key'].apply(
                lambda rk: pd.Series(race_key_to_year_round.get(rk, (None, None)))
            )
        
        # ═══════════════════════════════════════════════════════════
        # STEP 2: Resolve FKs for API data (needed for deduplication)
        # ═══════════════════════════════════════════════════════════
        api_data_resolved = api_data.copy()
        
        # driver_key
        api_data_resolved['driver_key'] = api_data_resolved['driver_ref'].map(driver_map)
        
        # race_key (composite lookup: year + round)
        api_data_resolved['race_key'] = api_data_resolved.apply(
            lambda row: race_map.get((row['race_year'], row['race_round'])),
            axis=1
        )
        
        # constructor_key (lookup from race_to_constructor mapping)
        api_data_resolved['constructor_key'] = api_data_resolved.apply(
            lambda row: constructor_map.get(
                race_to_constructor.get((row['race_year'], row['race_round'], row['driver_ref']))
            ),
            axis=1
        )
        
        # time_key (2-step: race_key → race_date → time_key)
        api_data_resolved['time_key'] = api_data_resolved['race_key'].map(race_to_date).map(time_map)
        
        # ═══════════════════════════════════════════════════════════
        # STEP 3: Deduplicate against CSV laps (CSV has priority)
        # ═══════════════════════════════════════════════════════════
        # Create composite key for matching
        csv_laps['_match_key'] = (
            csv_laps['race_year'].astype(str) + '_' +
            csv_laps['race_round'].astype(str) + '_' +
            csv_laps['driver_key'].astype(str) + '_' +
            csv_laps['lap'].astype(str)
        )
        
        api_data_resolved['_match_key'] = (
            api_data_resolved['race_year'].astype(str) + '_' +
            api_data_resolved['race_round'].astype(str) + '_' +
            api_data_resolved['driver_key'].astype(str) + '_' +
            api_data_resolved['lap'].astype(str)
        )
        
        csv_match_keys = set(csv_laps['_match_key'])
        
        # Filter: Keep only API laps NOT in CSV
        new_api_laps = api_data_resolved[~api_data_resolved['_match_key'].isin(csv_match_keys)].copy()
        
        print(f"\n  API laps processed: {len(api_data_resolved):,}")
        print(f"  API laps skipped (CSV exists): {len(api_data_resolved) - len(new_api_laps):,}")
        print(f"  API laps to add (new): {len(new_api_laps):,}")
        
        # Drop natural key columns (no longer needed after FK resolution)
        csv_laps = csv_laps.drop(columns=['race_year', 'race_round'], errors='ignore')
        new_api_laps = new_api_laps.drop(columns=['race_year', 'race_round', 'driver_ref'], errors='ignore')
        
        # Rename API columns to match CSV schema
        if 'lap_time' in new_api_laps.columns:
            new_api_laps = new_api_laps.rename(columns={'lap_time': 'lap_minutes'})
        
        # Debug: Check columns before alignment
        print(f"\n  [DEBUG] CSV columns ({len(csv_laps.columns)}): {sorted(csv_laps.columns.tolist())}")
        print(f"  [DEBUG] API columns ({len(new_api_laps.columns)}): {sorted(new_api_laps.columns.tolist())}")
        print(f"  [DEBUG] CSV dtypes:\n{csv_laps.dtypes}")
        print(f"  [DEBUG] API dtypes:\n{new_api_laps.dtypes}")
        
        # Align datetime precision (CSV uses datetime64[us], API uses datetime64[ns])
        if 'created_at' in new_api_laps.columns:
            new_api_laps['created_at'] = pd.to_datetime(new_api_laps['created_at']).astype('datetime64[us]')
        
        # Ensure both DataFrames have identical columns
        csv_cols = set(csv_laps.columns)
        api_cols = set(new_api_laps.columns)
        
        # Find column mismatches
        only_in_csv = csv_cols - api_cols
        only_in_api = api_cols - csv_cols
        
        if only_in_csv:
            print(f"  [DEBUG] Columns only in CSV: {only_in_csv}")
            # Add missing columns to API with NaN (except lap_key which gets regenerated)
            for col in only_in_csv:
                if col != 'lap_key':  # lap_key regenerated after merge
                    new_api_laps[col] = None
        
        if only_in_api:
            print(f"  [DEBUG] Columns only in API: {only_in_api}")
            # Add missing columns to CSV with NaN (except year which comes from API)
            for col in only_in_api:
                if col != 'year':  # year might not be in CSV but needed for partitioning
                    csv_laps[col] = None
                elif 'year' in only_in_api and 'year' not in csv_laps.columns:
                    # Add year column to CSV - derive from race_key if possible
                    csv_laps[col] = None  # Will be filled later
        
        # Align column order (use CSV as reference, but add 'year' if only in API)
        column_order = csv_laps.columns.tolist()
        if 'year' in new_api_laps.columns and 'year' not in column_order:
            column_order.append('year')
        
        # Filter column_order to only include columns that exist in new_api_laps
        column_order = [col for col in column_order if col in new_api_laps.columns]
        new_api_laps = new_api_laps[column_order]
        
        print(f"  [DEBUG] After alignment - CSV: {len(csv_laps.columns)} cols, API: {len(new_api_laps.columns)} cols")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 4: Combine CSV + new API laps
        # ═══════════════════════════════════════════════════════════
        new_laps = pd.concat([csv_laps, new_api_laps], ignore_index=True)
        
        # Drop temporary match key
        new_laps = new_laps.drop(columns=['_match_key'], errors='ignore')
    
    # ═══════════════════════════════════════════════════════════
    # STEP 5: Resolve FKs for all laps (if not already resolved)
    # ═══════════════════════════════════════════════════════════
    if 'driver_key' not in new_laps.columns:
        print("\nResolving foreign keys...")
        
        # driver_key
        new_laps['driver_key'] = new_laps['driver_ref'].map(driver_map)
        
        # race_key (composite lookup: year + round)
        new_laps['race_key'] = new_laps.apply(
            lambda row: race_map.get((row['race_year'], row['race_round'])),
            axis=1
        )
        
        # constructor_key (from race_to_constructor mapping)
        new_laps['constructor_key'] = new_laps.apply(
            lambda row: constructor_map.get(
                race_to_constructor.get((row['race_year'], row['race_round'], row['driver_ref']))
            ),
            axis=1
        )
        
        # time_key (2-step: race_key → race_date → time_key)
        new_laps['time_key'] = new_laps['race_key'].map(race_to_date).map(time_map)
    
    # ═══════════════════════════════════════════════════════════
    # STEP 6: Drop temporary FK lookup columns
    # ═══════════════════════════════════════════════════════════
    new_laps = new_laps.drop(columns=['driver_ref', 'race_year', 'race_round'], errors='ignore')
    
    # ═══════════════════════════════════════════════════════════
    # STEP 7: Regenerate lap_key (sequential surrogate key)
    # ═══════════════════════════════════════════════════════════
    new_laps = new_laps.drop(columns=['lap_key'], errors='ignore')
    new_laps = new_laps.sort_values(['year', 'race_key', 'lap', 'lap_position']).reset_index(drop=True)
    new_laps.insert(0, 'lap_key', range(1, len(new_laps) + 1))
    
    # Sort by lap_key for sequential order
    new_laps = new_laps.sort_values('lap_key').reset_index(drop=True)
    
    print(f"\nFinal merged dataset: {len(new_laps):,} lap times")
    print("="*70 + "\n")
    
    return new_laps


# ═══════════════════════════════════════════════════════════════
# AIRFLOW TASKS
# ═══════════════════════════════════════════════════════════════

def scrape_api_laps(**context):
    """
    Scrape lap times from Ergast API (2011-2025)
    
    Returns:
        List[Dict]: Raw API lap data (with XCom-safe datetime objects)
    """
    scraper = LapScraper()
    df_api = scraper.scrape_laps_range(BACKFILL_YEAR_START, BACKFILL_YEAR_END)
    
    # Transform to Silver schema (returns list of dicts with Python datetime)
    lap_records = scraper.transform_to_silver_schema(df_api)
    
    print(f"\nAPI scraping complete: {len(lap_records):,} lap times")
    
    # Return list of dicts (already XCom-compatible from lap.py)
    return lap_records


def backfill_laps_to_silver(**context):
    """
    Backfill lap times to Silver layer with FK resolution
    
    Steps:
    1. Load API data from XCom
    2. Load dimensions (driver, constructor, race, time)
    3. Build FK lookup maps
    4. Load existing Silver laps
    5. Merge (CSV priority)
    6. Write partitioned by year
    """
    print("\n" + "="*70)
    print("BACKFILLING LAP TIMES TO SILVER")
    print("="*70 + "\n")
    
    # ═══════════════════════════════════════════════════════════
    # STEP 1: Load API data from XCom
    # ═══════════════════════════════════════════════════════════
    ti = context['ti']
    api_data_list = ti.xcom_pull(task_ids='scrape_api_laps')
    
    # Debug: Check what XCom returned
    print(f"\n[DEBUG] XCom data type: {type(api_data_list)}")
    print(f"[DEBUG] XCom data length: {len(api_data_list) if api_data_list else 0}")
    if api_data_list and len(api_data_list) > 0:
        print(f"[DEBUG] First record keys: {list(api_data_list[0].keys())}")
        print(f"[DEBUG] First record sample: {api_data_list[0]}")
    
    df_api = pd.DataFrame(api_data_list)
    
    print(f"\n[DEBUG] DataFrame shape: {df_api.shape}")
    print(f"[DEBUG] DataFrame columns: {list(df_api.columns)}")
    print(f"[DEBUG] DataFrame dtypes:\n{df_api.dtypes}")
    
    print(f"\nAPI laps loaded from XCom: {len(df_api):,}")
    
    # ═══════════════════════════════════════════════════════════
    # STEP 2: Load dimensions from Silver
    # ═══════════════════════════════════════════════════════════
    minio = MinIOHelper()
    
    print("Loading dimensions for FK resolution...")
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
    
    print(f"  dim_driver: {len(dim_driver):,} records")
    print(f"  dim_constructor: {len(dim_constructor):,} records")
    print(f"  dim_race: {len(dim_race):,} records")
    print(f"  dim_time: {len(dim_time):,} records")
    
    # ═══════════════════════════════════════════════════════════
    # STEP 3: Build FK lookup maps
    # ═══════════════════════════════════════════════════════════
    print("\nBuilding FK lookup maps...")
    
    # driver_ref → driver_key
    driver_map = dict(zip(dim_driver['driver_ref'], dim_driver['driver_key']))
    
    # constructor_ref → constructor_key
    constructor_map = dict(zip(dim_constructor['constructor_ref'], dim_constructor['constructor_key']))
    
    # (race_year, race_round) → race_key
    race_map = {}
    for _, row in dim_race.iterrows():
        key = (row['race_year'], row['race_round'])
        race_map[key] = row['race_key']
    
    # race_key → (race_year, race_round) - REVERSE lookup for existing Silver data
    race_key_to_year_round = {}
    for _, row in dim_race.iterrows():
        race_key_to_year_round[row['race_key']] = (int(row['race_year']), int(row['race_round']))
    
    # race_key → race_date
    race_to_date = dict(zip(dim_race['race_key'], pd.to_datetime(dim_race['race_date']).dt.date))
    
    # date → time_key
    time_map = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))
    
    # (race_year, race_round, driver_ref) → constructor_ref
    # This is needed because API doesn't provide constructor info for laps
    # We need to get it from race results
    print("\nBuilding race-driver-constructor mapping from race results...")
    
    # Load results to get driver-constructor mapping per race
    try:
        results_partitions = minio.list_objects(SILVER_BUCKET, 'facts/fact_race_results/')
        race_to_constructor = {}
        
        # Build reverse lookups once outside the loop (race_key_to_year_round already built above)
        driver_key_to_ref = dict(zip(dim_driver['driver_key'], dim_driver['driver_ref']))
        constructor_key_to_ref = dict(zip(dim_constructor['constructor_key'], dim_constructor['constructor_ref']))

        for partition_path in results_partitions:
            if partition_path.endswith('.parquet'):
                results_df = minio.read_parquet(SILVER_BUCKET, partition_path)
                
                for _, result_row in results_df.iterrows():
                    race_key = result_row['race_key']
                    driver_key = result_row['driver_key']
                    constructor_key = result_row['constructor_key']
                    
                    if race_key in race_key_to_year_round and driver_key in driver_key_to_ref:
                        year, round_num = race_key_to_year_round[race_key]
                        driver_ref = driver_key_to_ref[driver_key]
                        constructor_ref = constructor_key_to_ref.get(constructor_key)
                        
                        key = (year, round_num, driver_ref)
                        race_to_constructor[key] = constructor_ref
        
        print(f"  Built race-driver-constructor mapping: {len(race_to_constructor):,} entries")
    
    except Exception as e:
        print(f"  WARNING: Could not load race results for constructor mapping: {e}")
        print("  Laps will have NULL constructor_key")
        race_to_constructor = {}
    
    # ═══════════════════════════════════════════════════════════
    # STEP 4: Load existing Silver laps (all partitions)
    # ═══════════════════════════════════════════════════════════
    print("\nLoading existing Silver laps...")
    
    try:
        # List all year partitions
        partitions = minio.list_objects(SILVER_BUCKET, 'facts/fact_laps/')
        existing_laps = []
        
        for partition_path in partitions:
            if partition_path.endswith('.parquet'):
                df_partition = minio.read_parquet(SILVER_BUCKET, partition_path)
                existing_laps.append(df_partition)
        
        if existing_laps:
            existing = pd.concat(existing_laps, ignore_index=True)
            print(f"Existing laps loaded: {len(existing):,}")
        else:
            existing = pd.DataFrame()
            print("No existing laps found")
    
    except Exception as e:
        print(f"No existing laps found (first run): {e}")
        existing = pd.DataFrame()
    
    # ═══════════════════════════════════════════════════════════
    # STEP 5: Merge existing + API laps
    # ═══════════════════════════════════════════════════════════
    merged = merge_laps(
        existing=existing,
        api_data=df_api,
        driver_map=driver_map,
        race_map=race_map,
        constructor_map=constructor_map,
        race_to_constructor=race_to_constructor,
        race_to_date=race_to_date,
        time_map=time_map,
        race_key_to_year_round=race_key_to_year_round
    )

    # ═══════════════════════════════════════════════════════════
    # STEP 5.5: Sort columns (consistent with all_silver_to_gold)
    # ═══════════════════════════════════════════════════════════
    column_order = [
        'lap_key',
        'race_key',
        'lap',
        'driver_key',
        'constructor_key',
        'time_key',
        'lap_position',
        'lap_minutes',
        'lap_milliseconds',
        'created_at',
        'source',
        'year'
    ]
    merged = merged[column_order]

    # ═══════════════════════════════════════════════════════════
    # STEP 6: Write to Silver (partitioned by year)
    # ═══════════════════════════════════════════════════════════
    print("Writing to Silver (partitioned by year)...")
    
    for year in merged['year'].unique():
        partition_data = merged[merged['year'] == year].copy()
        partition_path = f'facts/fact_laps/year={year}/data.parquet'
        
        minio.upload_parquet(partition_data, SILVER_BUCKET, partition_path)
        print(f"  {partition_path}: {len(partition_data):,} records")
    
    print(f"\nBackfill complete: {len(merged):,} lap times in Silver")
    
    # ═══════════════════════════════════════════════════════════
    # STEP 7: Summary for XCom
    # ═══════════════════════════════════════════════════════════
    summary = {
        'total_laps': len(merged),
        'csv_laps': len(merged[merged['source'] == 'f1_data.csv']),
        'api_laps': len(merged[merged['source'] == 'api']),
        'years_covered': f"{BACKFILL_YEAR_START}-{BACKFILL_YEAR_END}",
        'partitions_written': len(merged['year'].unique())
    }
    
    print("\n" + "="*70)
    print("BACKFILL SUMMARY")
    print("="*70)
    for key, value in summary.items():
        print(f"{key}: {value}")
    print("="*70 + "\n")
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='backfill_summary', value=summary)
    
    return summary


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='api_laps_bronze_to_silver',
    default_args=default_args,
    description='Backfill F1 lap times from Ergast API 2025 to Silver',
    schedule=None,  # Manual trigger only (massive data volume)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver','fact','minio', 'api']
) as dag:
    
    task_scrape = PythonOperator(
        task_id='scrape_api_laps',
        python_callable=scrape_api_laps,
        execution_timeout=timedelta(hours=3)  # ~100 minutes minimum for rate limiting
    )
    
    task_backfill = PythonOperator(
        task_id='backfill_laps_to_silver',
        python_callable=backfill_laps_to_silver,
        execution_timeout=timedelta(hours=1)
    )
    
    task_scrape >> task_backfill
