# dags/master_f1_etl_pipeline.py

"""
F1 ETL Master Pipeline: Bronze → Silver → Gold

Architecture:
- Orchestrates all sub-pipelines in sequence
- Each pipeline must complete before triggering the next
- Idempotent: safe to re-run at any time
- Monitoring: Single DAG for overall pipeline health

Pipeline Flow:
1. csv_bronze_to_silver (CSV → Silver dimensions + facts)
2. api_constructors_bronze_to_silver (API constructors → Dimension Silver)
3. api_drivers_bronze_to_silver (API drivers → Dimension Silver)
4. api_circuits_bronze_to_silver (API circuits → Dimension Silver)
5. api_races_bronze_to_silver (API races → Dimension Silver)
6. api_results_bronze_to_silver (API results → Fact Silver)
7. api_laps_bronze_to_silver (API laps → Fact Silver)
8. api_pitstops_bronze_to_silver (API pitstops → Fact Silver)
9. api_driverstandings_bronze_to_silver (API driver standings → Fact Silver)
10. api_constructorstandings_bronze_to_silver (API constructor standings → Fact Silver)
11. all_silver_to_gold (Silver → Gold PostgreSQL)

Dependencies:
- CSV must complete first (creates dimensions from CSV)
- API dimension pipelines (2-5) run in PARALLEL after CSV
- API fact pipelines (6-10) depend on all dimensions
- Gold layer depends on all Silver data being ready
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def print_pipeline_start(**context):
    """Log pipeline start"""
    print("\n" + "=" * 80)
    print("F1 ETL MASTER PIPELINE STARTED")
    print("=" * 80)
    print("\nPipeline Flow:")
    print("  1. CSV → Silver (Dimensions + Facts)")
    print("  2-5. API Dimensions → Silver (PARALLEL)")
    print("     ├─ Constructors")
    print("     ├─ Drivers")
    print("     ├─ Circuits")
    print("     └─ Races")
    print("  6-10. API Facts → Silver (PARALLEL)")
    print("      ├─ Results")
    print("      ├─ Laps")
    print("      ├─ Pitstops")
    print("      ├─ Driver Standings")
    print("      └─ Constructor Standings")
    print("  11. Silver → Gold (PostgreSQL)")
    print("\n" + "=" * 80 + "\n")


def print_pipeline_complete(**context):
    """Log pipeline completion"""
    print("\n" + "=" * 80)
    print("F1 ETL MASTER PIPELINE COMPLETED SUCCESSFULLY! ")
    print("=" * 80)
    print("\nAll data loaded:")
    print("   Silver Layer: Bronze CSV + API data in MinIO S3")
    print("   Gold Layer: PostgreSQL with proper data types and constraints")
    print("   Dimensions: driver, constructor, circuit, race, time")
    print("   Facts: race_results, laps, pitstops, driver_standings, constructor_standings")
    print("\nData Summary:")
    print("  - Total Dimensions loaded")
    print("  - Total Facts loaded (CSV + API)")
    print("  - All surrogate keys generated")
    print("  - All foreign keys resolved")
    print("\n" + "=" * 80 + "\n")


# ═══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id='master_f1_etl_pipeline',
    default_args=default_args,
    description='F1 ETL Master Pipeline: Bronze CSV + API → Silver MinIO S3 → Gold PostgreSQL',
    schedule=None,  # Manual trigger only (uncomment to schedule)
    # schedule='0 2 * * *',  # Daily at 2:00 AM UTC if you want automatic scheduling
    start_date=datetime(2026, 2, 11),
    catchup=False,
    tags=['0.master', '1.orchestration', 'f1-etl', 'bronze-silver-gold'],
) as dag:
    
    # ═════════════════════════════════════════════════════════════════════════════
    # LOGGING TASKS
    # ═════════════════════════════════════════════════════════════════════════════
    start_pipeline = PythonOperator(
        task_id='start_pipeline',
        python_callable=print_pipeline_start,
        doc='Log pipeline start time and flow'
    )

    # ═════════════════════════════════════════════════════════════════════════════
    # STEP 1: CSV → Silver (Dimensions + Facts from CSV file)
    # ═════════════════════════════════════════════════════════════════════════════
    step_1_csv_bronze_to_silver = TriggerDagRunOperator(
        task_id='step_1_csv_bronze_to_silver',
        trigger_dag_id='csv_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True, 
        poke_interval=30,
        execution_timeout=timedelta(hours=2),
        doc='Extract Bronze CSV, transform to Silver dimensions and facts, load to MinIO S3'
    )

    # ═════════════════════════════════════════════════════════════════════════════
    # STEP 2-5: API Dimensions → Silver (PARALLEL after Step 1)
    # These 4 pipelines run simultaneously to speed up execution
    # ═════════════════════════════════════════════════════════════════════════════
    
    step_2_api_constructors = TriggerDagRunOperator(
        task_id='step_2_api_constructors_bronze_to_silver',
        trigger_dag_id='api_constructors_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for constructors, transform to Silver dimension, load to MinIO S3'
    )

    step_3_api_drivers = TriggerDagRunOperator(
        task_id='step_3_api_drivers_bronze_to_silver',
        trigger_dag_id='api_drivers_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for drivers, transform to Silver dimension, load to MinIO S3'
    )

    step_4_api_circuits = TriggerDagRunOperator(
        task_id='step_4_api_circuits_bronze_to_silver',
        trigger_dag_id='api_circuits_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for circuits, transform to Silver dimension, load to MinIO S3'
    )

    step_5_api_races = TriggerDagRunOperator(
        task_id='step_5_api_races_bronze_to_silver',
        trigger_dag_id='api_races_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for races, transform to Silver dimension, load to MinIO S3'
    )

    # ═════════════════════════════════════════════════════════════════════════════
    # STEP 6-10: API Facts → Silver (PARALLEL after all dimensions)
    # These 5 pipelines run simultaneously after all dimensions are ready
    # ═════════════════════════════════════════════════════════════════════════════
    
    step_6_api_results = TriggerDagRunOperator(
        task_id='step_6_api_results_bronze_to_silver',
        trigger_dag_id='api_results_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for race results, merge with CSV, load to MinIO S3 (partitioned by year)'
    )

    step_7_api_laps = TriggerDagRunOperator(
        task_id='step_7_api_laps_bronze_to_silver',
        trigger_dag_id='api_laps_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for laps, merge with CSV, load to MinIO S3'
    )

    step_8_api_pitstops = TriggerDagRunOperator(
        task_id='step_8_api_pitstops_bronze_to_silver',
        trigger_dag_id='api_pitstops_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for pitstops, merge with CSV, load to MinIO S3'
    )

    step_9_api_dstandings = TriggerDagRunOperator(
        task_id='step_9_api_driverstandings_bronze_to_silver',
        trigger_dag_id='api_driverstandings_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for driver standings, merge with CSV, load to MinIO S3'
    )

    step_10_api_cstandings = TriggerDagRunOperator(
        task_id='step_10_api_constructorstandings_bronze_to_silver',
        trigger_dag_id='api_constructorstandings_bronze_to_silver',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=3),
        doc='Scrape Ergast API for constructor standings, merge with CSV, load to MinIO S3'
    )

    # ═════════════════════════════════════════════════════════════════════════════
    # STEP 11: Silver → Gold (PostgreSQL)
    # Depends on all Silver data being ready (dimensions + facts)
    # ═════════════════════════════════════════════════════════════════════════════
    step_11_silver_to_gold = TriggerDagRunOperator(
        task_id='step_11_silver_to_gold',
        trigger_dag_id='all_silver_to_gold',
        wait_for_completion=True,
        deferrable=True,  
        poke_interval=30,
        execution_timeout=timedelta(hours=2),
        doc='Load all Silver data (MinIO S3) to Gold layer (PostgreSQL) with proper data types and constraints'
    )

    # ═════════════════════════════════════════════════════════════════════════════
    # LOGGING TASKS
    # ═════════════════════════════════════════════════════════════════════════════
    end_pipeline = PythonOperator(
        task_id='end_pipeline',
        python_callable=print_pipeline_complete,
        doc='Log pipeline completion and summary'
    )

    # ═════════════════════════════════════════════════════════════════════════════
    # DEPENDENCIES - Task Execution Order
    # ═════════════════════════════════════════════════════════════════════════════
    
    # 1. Pipeline starts with logging
    start_pipeline >> step_1_csv_bronze_to_silver
    
    # 2. CSV must complete before API dimensions start (all 4 in parallel)
    step_1_csv_bronze_to_silver >> [
        step_2_api_constructors,
        step_3_api_drivers,
        step_4_api_circuits,
        step_5_api_races
    ]
    
    # 3. All API dimensions must complete before API facts start
    # Each dimension → all facts (creates cross-product dependency)
    step_2_api_constructors >> [
        step_6_api_results,
        step_7_api_laps,
        step_8_api_pitstops,
        step_9_api_dstandings,
        step_10_api_cstandings
    ]
    
    step_3_api_drivers >> [
        step_6_api_results,
        step_7_api_laps,
        step_8_api_pitstops,
        step_9_api_dstandings,
        step_10_api_cstandings
    ]
    
    step_4_api_circuits >> [
        step_6_api_results,
        step_7_api_laps,
        step_8_api_pitstops,
        step_9_api_dstandings,
        step_10_api_cstandings
    ]
    
    step_5_api_races >> [
        step_6_api_results,
        step_7_api_laps,
        step_8_api_pitstops,
        step_9_api_dstandings,
        step_10_api_cstandings
    ]
    
    # 4. All API facts must complete before Silver→Gold transformation
    [
        step_6_api_results,
        step_7_api_laps,
        step_8_api_pitstops,
        step_9_api_dstandings,
        step_10_api_cstandings
    ] >> step_11_silver_to_gold
    
    # 5. Pipeline ends with logging
    step_11_silver_to_gold >> end_pipeline