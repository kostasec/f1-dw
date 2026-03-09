# dags/weekly_f1_pipeline.py

"""
F1 Weekly Pipeline: Producer (Ergast API -> Silver -> Kafka) + Consumer (Kafka -> Gold PostgreSQL)

Purpose:
    Single DAG that runs both producer and consumer phases sequentially.
    Producer scrapes 2026+ F1 data, merges into Silver, publishes to Kafka.
    Consumer reads from Kafka topics and inserts into PostgreSQL Gold.

Schedule:
    Every Monday at 08:00 UTC.

Architecture:
    TaskGroup 'producer':
        1. Scrape 2026+ data from Ergast API
        2. Identify NEW records (not yet in Silver MinIO)
        3. Assign surrogate keys (continuing from max existing key)
        4. Merge into Silver Parquet files (idempotent)
        5. Publish ONLY new records to Kafka topics

    TaskGroup 'consumer':
        6. Read new messages from Kafka topics
        7. INSERT into PostgreSQL Gold (append-only, never drops tables)
        8. Commit Kafka offsets after successful insert

Kafka Topics:
    Dimensions : f1.dim.drivers, f1.dim.constructors, f1.dim.circuits, f1.dim.races
    Facts      : f1.fact.results, f1.fact.laps, f1.fact.pitstops,
                 f1.fact.driver_standings, f1.fact.constructor_standings
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import sys
sys.path.insert(0, '/opt/airflow/dags')

# Reuse all task functions from existing pipelines
from kafka_producer_pipeline import (
    scrape_publish_drivers,
    scrape_publish_constructors,
    scrape_publish_circuits,
    scrape_publish_races,
    scrape_publish_results,
    scrape_publish_laps,
    scrape_publish_pitstops,
    scrape_publish_driver_standings,
    scrape_publish_constructor_standings,
    print_producer_summary,
)
from kafka_consumer_pipeline import (
    consume_drivers,
    consume_constructors,
    consume_circuits,
    consume_races,
    consume_results,
    consume_laps,
    consume_pitstops,
    consume_driver_standings,
    consume_constructor_standings,
    print_consumer_summary,
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='weekly_f1_pipeline',
    default_args=default_args,
    description='F1 Weekly: Producer (API -> Silver -> Kafka) then Consumer (Kafka -> Gold PostgreSQL)',
    schedule='0 8 * * 1',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['kafka', 'producer', 'consumer', 'unified', 'f1-etl'],
) as dag:

    # ═════════════════════════════════════════════════════════════════════════
    # PRODUCER TASK GROUP
    # ═════════════════════════════════════════════════════════════════════════
    with TaskGroup('producer', tooltip='Ergast API -> Silver -> Kafka') as producer_group:

        # Dimension tasks
        task_drivers = PythonOperator(
            task_id='scrape_publish_drivers',
            python_callable=scrape_publish_drivers,
        )
        task_constructors = PythonOperator(
            task_id='scrape_publish_constructors',
            python_callable=scrape_publish_constructors,
        )
        task_circuits = PythonOperator(
            task_id='scrape_publish_circuits',
            python_callable=scrape_publish_circuits,
        )
        task_races = PythonOperator(
            task_id='scrape_publish_races',
            python_callable=scrape_publish_races,
        )

        # Fact tasks
        task_results = PythonOperator(
            task_id='scrape_publish_results',
            python_callable=scrape_publish_results,
        )
        task_laps = PythonOperator(
            task_id='scrape_publish_laps',
            python_callable=scrape_publish_laps,
        )
        task_pitstops = PythonOperator(
            task_id='scrape_publish_pitstops',
            python_callable=scrape_publish_pitstops,
        )
        task_dstandings = PythonOperator(
            task_id='scrape_publish_driver_standings',
            python_callable=scrape_publish_driver_standings,
        )
        task_cstandings = PythonOperator(
            task_id='scrape_publish_constructor_standings',
            python_callable=scrape_publish_constructor_standings,
        )

        task_producer_summary = PythonOperator(
            task_id='print_summary',
            python_callable=print_producer_summary,
        )

        # Dependencies within producer
        task_circuits >> task_races
        independent_dims = [task_drivers, task_constructors, task_circuits]
        facts = [task_results, task_laps, task_pitstops, task_dstandings, task_cstandings]
        for dim in independent_dims:
            dim >> facts
        task_races >> facts
        facts >> task_producer_summary

    # ═════════════════════════════════════════════════════════════════════════
    # CONSUMER TASK GROUP
    # ═════════════════════════════════════════════════════════════════════════
    with TaskGroup('consumer', tooltip='Kafka -> Gold PostgreSQL') as consumer_group:

        # Dimension consume tasks
        task_consume_drivers = PythonOperator(
            task_id='consume_drivers',
            python_callable=consume_drivers,
        )
        task_consume_constructors = PythonOperator(
            task_id='consume_constructors',
            python_callable=consume_constructors,
        )
        task_consume_circuits = PythonOperator(
            task_id='consume_circuits',
            python_callable=consume_circuits,
        )
        task_consume_races = PythonOperator(
            task_id='consume_races',
            python_callable=consume_races,
        )

        # Fact consume tasks
        task_consume_results = PythonOperator(
            task_id='consume_results',
            python_callable=consume_results,
        )
        task_consume_laps = PythonOperator(
            task_id='consume_laps',
            python_callable=consume_laps,
        )
        task_consume_pitstops = PythonOperator(
            task_id='consume_pitstops',
            python_callable=consume_pitstops,
        )
        task_consume_dstandings = PythonOperator(
            task_id='consume_driver_standings',
            python_callable=consume_driver_standings,
        )
        task_consume_cstandings = PythonOperator(
            task_id='consume_constructor_standings',
            python_callable=consume_constructor_standings,
        )

        task_consumer_summary = PythonOperator(
            task_id='print_summary',
            python_callable=print_consumer_summary,
        )

        # Dependencies within consumer
        dims = [task_consume_drivers, task_consume_constructors,
                task_consume_circuits, task_consume_races]
        facts = [task_consume_results, task_consume_laps, task_consume_pitstops,
                 task_consume_dstandings, task_consume_cstandings]
        for dim in dims:
            dim >> facts
        facts >> task_consumer_summary

    # ═════════════════════════════════════════════════════════════════════════
    # PIPELINE FLOW: producer -> consumer
    # ═════════════════════════════════════════════════════════════════════════
    producer_group >> consumer_group
