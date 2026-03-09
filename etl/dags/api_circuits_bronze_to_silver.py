from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')

from utils.s3_helper import MinIOHelper
from scraper.circuit import CircuitScraper

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

SILVER_BUCKET = 'f1-silver'

# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def circuits_equal(row1: pd.Series, row2: pd.Series) -> bool:

    compare_fields = [
        'circuit_name',
        'circuit_city',
        'circuit_country',
        'circuit_latitude',
        'circuit_longtitude',
        'circuit_url'
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

def _should_skip_csv_circuit(existing_row: pd.Series, circuit_references: str) -> bool:
    """Check if circuit should be skipped due to CSV priority"""
    if existing_row['source'] == 'f1_data.csv':
        print(f"SKIP: {circuit_references:20s} (CSV priority)")
        return True
    return False

def _should_skip_duplicate(existing_row: pd.Series, new_row: pd.Series, circuit_references: str) -> bool:
    """Check if API constructor is identical and should be skipped"""
    if circuits_equal(existing_row, new_row):
        print(f"SKIP: {circuit_references:20s} (duplicate, no change)")
        return True
    return False

def _update_existing_circuit(merged: pd.DataFrame, existing_row: pd.Series,
                                new_row: pd.Series, circuit_references: str) -> None:
    """Update existing API circuits with new data"""
    print(f"UPDATE: {circuit_references:20s} (API refresh, data changed)")
    
    # PRESERVE existing metadata
    new_row['circuit_key'] = existing_row['circuit_key']
    new_row['circuit_id'] = existing_row['circuit_id']
    new_row['source'] = existing_row['source']
    new_row['created_at'] = existing_row['created_at']

    # Update row in merged DataFrame - use column-by-column assignment to preserve dtypes
    mask = merged['circuit_references'] == circuit_references
    for col in new_row.index:
        merged.loc[mask, col] = new_row[col]

def _add_new_circuit(merged: pd.DataFrame, new_row: pd.Series,
                     circuit_references: str, max_key: int) -> int:
    """Add new API circuit to merged DataFrame"""
    print(f"ADD: {circuit_references:20s} (new circuit from API, circuit_id{new_row['circuit_id']})")

    # Generate NEW surrogate key
    new_key = max_key + 1
    new_row['circuit_key'] = new_key
    new_row['created_at'] = pd.Timestamp.now()

    # Append to merged DataFrame (in-place via parent scope)
    merged_updated = pd.concat([merged, new_row.to_frame().T], ignore_index=True)
    return new_key, merged_updated


def _process_api_circuit(merged: pd.DataFrame, new_row: pd.Series, max_key: int) -> tuple:
    
    circuit_references = new_row['circuit_references']

    #Circuit dosen't exist - ADD new
    if circuit_references not in merged['circuit_references'].values:
        new_key, merged = _add_new_circuit(merged, new_row, circuit_references, max_key)
        return new_key, merged
    
    # Circuit exists - check action
    existing_row = merged[merged['circuit_references'] == circuit_references].iloc[0]

    # CSV has priority - SKIP
    if _should_skip_csv_circuit(existing_row, circuit_references):
        return max_key, merged

    # API constructor - check if duplicate
    if _should_skip_duplicate(existing_row, new_row, circuit_references):
        return max_key, merged
    
    # Data changed - UPDATE
    _update_existing_circuit(merged, existing_row, new_row, circuit_references)
    return max_key, merged

def merge_circuits(existing: pd.DataFrame, new_api: pd.DataFrame) -> pd.DataFrame:

    print("\n  Merge Configuration:")
    print(f"Existing constructors: {len(existing):,}")
    print(f"New API constructors: {len(new_api)}")
    print(f"Merge key: constructor_ref (PRIMARY natural key)")
    print(f"CSV priority: YES (skip API for CSV constructors)")

    # EDGE CASE: First run (no existing data)
    if existing.empty:
        print("\n FIRST RUN: No existing data, adding all API circuits")
        new_api = new_api.copy()
        new_api['circuit_key'] = range(1, len(new_api) + 1)
        print(f"Generated circuit_key: 1-{len(new_api)}")
        print(f"circuit_id already set: {new_api['circuit_id'].min()}-{new_api['circuit_id'].max()}")
        return new_api

    # MAIN CASE: Incremental merge
    merged = existing.copy()
    max_circuit_key = merged['circuit_key'].max()
    print(f"\n  Current max circuit_key: {max_circuit_key}")
    print(f"\n  Processing {len(new_api)} API constructors:\n")

    # Process each API circuit
    for idx, new_row in new_api.iterrows():
        max_circuit_key, merged = _process_api_circuit(merged, new_row, max_circuit_key)
    
    return merged

# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE FUNCTION
# ═══════════════════════════════════════════════════════════════

def backfill_dim_circuit(**context):

    print("=" * 70)
    print("F1 CIRCUIT BACKFILL: ERGAST API → SILVER")
    print("=" * 70)

    minio=MinIOHelper()
    scraper = CircuitScraper()

    # ═══════════════════════════════════════════════════════════════
    # STEP 1: SCRAPE API
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 1] Scraping API...")

    df_api_raw = scraper.scrape_all_circuits()
    
    print(f"API scraping complete: {len(df_api_raw):,} circuit records")

    # ═══════════════════════════════════════════════════════════════
    # STEP 2: TRANSFORM to Silver schema
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 2] Transforming to Silver schema...")    

    df_api_transformed = scraper.transform_to_silver_schema(df_api_raw)
    
    print(f"Transformation complete: {len(df_api_transformed)} unique constructors")
    print(f"circuit_id range: {df_api_transformed['circuit_id'].min()} - {df_api_transformed['circuit_id'].max()}")

    # ══════════════════════════════════════════════════════════════
    # STEP 3: LOAD existing Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 3] Loading existing Silver data...")

    try:
        existing_silver = minio.read_parquet(
            SILVER_BUCKET,
            'dimensions/dim_circuit/dim_circuit.parquet'
        )
        print(f"Loaded {len(existing_silver):,} existing circuits from Silver")
        
        # Show source breakdown
        if not existing_silver.empty:
            source_counts = existing_silver['source'].value_counts().to_dict()
            print(f"Existing sources: {source_counts}")
            
            # Show constructor_id ranges
            csv_circuits = existing_silver[existing_silver['source'] == 'f1_data.csv']
            if not csv_circuits.empty:
                print(f"CSV circuit_id range: {csv_circuits['circuit_id'].min()} - {csv_circuits['circuit_id'].max()}")
    
    except Exception as e:
        print(f"No existing Silver data found (first run)")
        print(f"Details: {e}")
        existing_silver = pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════
    # STEP 4: MERGE (incremental, idempotent)
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 4] Merging API data with existing Silver...")
    
    merged = merge_circuits(existing_silver, df_api_transformed)
    
    print(f"Merge complete: {len(merged):,} total constructors")

    # ═══════════════════════════════════════════════════════════════
    # STEP 5: WRITE to Silver
    # ═══════════════════════════════════════════════════════════════
    print("\n[STEP 5] Writing to Silver layer...")

    column_order = [
        'circuit_key',
        'circuit_id',
        'circuit_references',
        'circuit_name',
        'circuit_city',
        'circuit_country',
        'circuit_latitude',
        'circuit_longtitude',
        'circuit_altitude',
        'circuit_url',
        'created_at',
        'source'
    ]

    merged = merged[column_order]

    minio.upload_parquet(
        merged,
        SILVER_BUCKET,
        'dimensions/dim_circuit/dim_circuit.parquet'
    )

    print(f"Written to: s3://{SILVER_BUCKET}/dimensions/dim_circuit/dim_circuit.parquet")

    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    summary = {
        'existing_count': len(existing_silver),
        'api_scraped': len(df_api_transformed),
        'final_count': len(merged),
        'new_circuits_added': len(merged) - len(existing_silver) if not existing_silver.empty else len(merged)
    }

    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"  Existing circuits in Silver: {summary['existing_count']:,}")
    print(f"  API circuits scraped: {summary['api_scraped']}")
    print(f"  Final constructor count: {summary['final_count']:,}")
    print(f"  New constructors added: {summary['new_circuits_added']}")

    # Source breakdown
    source_final = merged['source'].value_counts().to_dict()
    print(f"\n  Final source breakdown:")
    for source, count in source_final.items():
        print(f"    {source:15s}: {count:,}")

    # constructor_id ranges
    print(f"\n  circuit_id ranges:")
    for source in merged['source'].unique():
        source_data = merged[merged['source'] == source]
        print(f"{source:15s}: {source_data['circuit_id'].min()} - {source_data['circuit_id'].max()}")
    
    print("=" * 70)

    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='backfill_summary', value=summary)
    
    return summary

# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='api_circuits_bronze_to_silver',
    default_args=default_args,
    description='F1 ETL: API Bronze → Silver | Backfill constructors 2025',
    schedule=None,
    start_date=datetime(2026, 2, 7),
    catchup=False,
    tags=['silver','dimension','minio', 'api'],
) as dag:
    
    task_backfill = PythonOperator(
        task_id='backfill_dim_circuit',
        python_callable=backfill_dim_circuit
    )