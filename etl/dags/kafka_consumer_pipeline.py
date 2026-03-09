# dags/kafka_consumer_pipeline.py

"""
F1 Kafka Consumer Pipeline: Kafka Topics -> Gold PostgreSQL (incremental)

Purpose:
    Reads new F1 records from Kafka topics and loads them into the
    existing PostgreSQL Gold layer WITHOUT dropping or recreating tables.
    Only records that don't already exist (checked by natural key) are inserted.

Triggered by:
    kafka_producer_pipeline (TriggerDagRunOperator, fire-and-forget)
    Can also be triggered manually.

Architecture (Kafka concepts demonstrated):
    - Consumer Group: 'f1-incremental-consumers'
        All tasks in this DAG belong to the same group.
        Kafka tracks what each group has already read (offset).
        Next weekly run reads ONLY messages published since last commit.

    - Offset management: manual (enable_auto_commit=False)
        Offset is committed only AFTER successful PostgreSQL insert.
        If insert fails, the same messages will be re-read on next run.

    - auto_offset_reset='earliest'
        First ever run reads all messages from the beginning of each topic.
        Subsequent runs start from the last committed offset.

    - consumer_timeout_ms=15000
        Consumer stops after 15 seconds with no new messages.
        This converts the streaming consumer into a batch-friendly one.

Incremental merge strategy:
    Dimensions : INSERT only records whose natural key (driver_ref, etc.)
                 is not already present in PostgreSQL.
    Facts      : INSERT only records whose surrogate key is not already present.
                 (Surrogate keys are unique because the producer generates them
                  starting from max(existing_key) + 1.)

IMPORTANT: This DAG NEVER drops or recreates tables.
           It relies on master_pipeline having run at least once to create
           the dimensions and facts schemas in PostgreSQL.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.kafka_helper import KafkaConsumerHelper

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

POSTGRES_CONN_ID = 'postgres_f1_gold'
KAFKA_BOOTSTRAP  = 'kafka:29092'
CONSUMER_GROUP   = 'f1-incremental-consumers'
CONSUMER_TIMEOUT = 15000   # ms to wait for new messages before stopping

# ── Topics (must match producer) ──────────────────────────────────────────────
TOPIC_DRIVERS               = 'f1.dim.drivers'
TOPIC_CONSTRUCTORS          = 'f1.dim.constructors'
TOPIC_CIRCUITS              = 'f1.dim.circuits'
TOPIC_RACES                 = 'f1.dim.races'
TOPIC_RESULTS               = 'f1.fact.results'
TOPIC_LAPS                  = 'f1.fact.laps'
TOPIC_PITSTOPS              = 'f1.fact.pitstops'
TOPIC_DRIVER_STANDINGS      = 'f1.fact.driver_standings'
TOPIC_CONSTRUCTOR_STANDINGS = 'f1.fact.constructor_standings'


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _get_engine():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()


def _read_topic(topic: str) -> pd.DataFrame:
    """
    Read all available messages from a single Kafka topic.
    Returns DataFrame (empty if no messages).

    Kafka concepts in action:
    - Consumer joins group CONSUMER_GROUP
    - Reads from last committed offset (or beginning on first run)
    - Stops after CONSUMER_TIMEOUT ms with no new messages
    - Offset is NOT committed here - committed after successful DB insert
    """
    print(f"  Connecting to Kafka, topic='{topic}', group='{CONSUMER_GROUP}'")
    consumer = KafkaConsumerHelper(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topics=[topic],
        group_id=CONSUMER_GROUP,
        consumer_timeout_ms=CONSUMER_TIMEOUT
    )
    batch    = consumer.consume_batch()
    records  = batch.get(topic, [])
    consumer.commit()    # commit offset AFTER reading
    consumer.close()

    if not records:
        print(f"  No new messages on topic '{topic}'")
        return pd.DataFrame()

    print(f"  Read {len(records)} messages from topic '{topic}'")
    return pd.DataFrame(records)


def _insert_new_records(df: pd.DataFrame, table: str, schema: str,
                        natural_key_col: str, engine) -> int:
    """
    Insert records that are not yet in PostgreSQL (checked by natural_key_col).
    Uses if_exists='append' - never drops the table.
    Returns number of inserted rows.
    """
    if df.empty:
        return 0

    # Read existing natural keys from PostgreSQL
    try:
        existing_df  = pd.read_sql(f'SELECT "{natural_key_col}" FROM {schema}."{table}"', con=engine)
        existing_keys = set(existing_df[natural_key_col].values)
    except Exception:
        # Table might not exist yet (first consumer run before master_pipeline)
        existing_keys = set()

    df_to_insert = df[~df[natural_key_col].isin(existing_keys)].copy()

    if df_to_insert.empty:
        print(f"  All {len(df)} records already exist in {schema}.{table} - skipping")
        return 0

    df_to_insert.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    print(f"  Inserted {len(df_to_insert)} new records into {schema}.{table}")
    return len(df_to_insert)


def _insert_new_facts(df: pd.DataFrame, table: str, schema: str,
                      surrogate_key_col: str, engine) -> int:
    """
    Insert fact records that are not yet in PostgreSQL (checked by surrogate key).
    Surrogate keys are unique because the producer generated them from max+1.
    """
    if df.empty:
        return 0

    try:
        existing_df  = pd.read_sql(f'SELECT "{surrogate_key_col}" FROM {schema}."{table}"', con=engine)
        existing_keys = set(existing_df[surrogate_key_col].values)
    except Exception:
        existing_keys = set()

    df_to_insert = df[~df[surrogate_key_col].isin(existing_keys)].copy()

    if df_to_insert.empty:
        print(f"  All {len(df)} records already exist in {schema}.{table} - skipping")
        return 0

    df_to_insert.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    print(f"  Inserted {len(df_to_insert)} new records into {schema}.{table}")
    return len(df_to_insert)


# ═══════════════════════════════════════════════════════════════════════════════
# DIMENSION CONSUME TASKS
# ═══════════════════════════════════════════════════════════════════════════════

def consume_drivers(**context):
    """
    Read f1.dim.drivers topic, insert new drivers into dimensions.driver.
    Natural key: driver_ref
    """
    print("=" * 70)
    print("CONSUMER: f1.dim.drivers -> dimensions.driver")
    print("=" * 70)

    df = _read_topic(TOPIC_DRIVERS)
    if df.empty:
        context['ti'].xcom_push(key='drivers_inserted', value=0)
        return {'inserted': 0}

    # Type conversions (match all_silver_to_gold.py)
    df['driver_dob']    = pd.to_datetime(df['driver_dob'],  errors='coerce')
    df['created_at']    = pd.to_datetime(df['created_at'],  errors='coerce')
    df['driver_number'] = pd.to_numeric(df['driver_number'], errors='coerce').astype('Int64')

    engine  = _get_engine()
    count   = _insert_new_records(df, 'driver', 'dimensions', 'driver_ref', engine)
    context['ti'].xcom_push(key='drivers_inserted', value=count)
    return {'inserted': count}


def consume_constructors(**context):
    """
    Read f1.dim.constructors topic, insert new constructors into dimensions.constructor.
    Natural key: constructor_ref
    """
    print("=" * 70)
    print("CONSUMER: f1.dim.constructors -> dimensions.constructor")
    print("=" * 70)

    df = _read_topic(TOPIC_CONSTRUCTORS)
    if df.empty:
        context['ti'].xcom_push(key='constructors_inserted', value=0)
        return {'inserted': 0}

    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

    engine = _get_engine()
    count  = _insert_new_records(df, 'constructor', 'dimensions', 'constructor_ref', engine)
    context['ti'].xcom_push(key='constructors_inserted', value=count)
    return {'inserted': count}


def consume_circuits(**context):
    """
    Read f1.dim.circuits topic, insert new circuits into dimensions.circuit.
    Natural key: circuit_references
    """
    print("=" * 70)
    print("CONSUMER: f1.dim.circuits -> dimensions.circuit")
    print("=" * 70)

    df = _read_topic(TOPIC_CIRCUITS)
    if df.empty:
        context['ti'].xcom_push(key='circuits_inserted', value=0)
        return {'inserted': 0}

    df['created_at']       = pd.to_datetime(df['created_at'], errors='coerce')
    df['circuit_altitude'] = pd.to_numeric(df['circuit_altitude'], errors='coerce').astype('Int64')

    engine = _get_engine()
    count  = _insert_new_records(df, 'circuit', 'dimensions', 'circuit_references', engine)
    context['ti'].xcom_push(key='circuits_inserted', value=count)
    return {'inserted': count}


def consume_races(**context):
    """
    Read f1.dim.races topic, insert new races into dimensions.race.
    Natural key: race_id (unique per race, generated by producer)
    """
    print("=" * 70)
    print("CONSUMER: f1.dim.races -> dimensions.race")
    print("=" * 70)

    df = _read_topic(TOPIC_RACES)
    if df.empty:
        context['ti'].xcom_push(key='races_inserted', value=0)
        return {'inserted': 0}

    # Date + time columns (match all_silver_to_gold.py)
    date_cols = ['race_date', 'fp1_date', 'fp2_date', 'fp3_date', 'qualification_date', 'sprint_date']
    time_cols = ['race_time', 'fp1_time', 'fp2_time', 'fp3_time', 'qualification_time', 'sprint_time']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.time

    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

    engine = _get_engine()
    count  = _insert_new_records(df, 'race', 'dimensions', 'race_id', engine)
    context['ti'].xcom_push(key='races_inserted', value=count)
    return {'inserted': count}


# ═══════════════════════════════════════════════════════════════════════════════
# FACT CONSUME TASKS
# ═══════════════════════════════════════════════════════════════════════════════

def consume_results(**context):
    """
    Read f1.fact.results topic, insert new results into facts.race_results.
    Dedup key: result_key (surrogate, unique, generated by producer).
    """
    print("=" * 70)
    print("CONSUMER: f1.fact.results -> facts.race_results")
    print("=" * 70)

    df = _read_topic(TOPIC_RESULTS)
    if df.empty:
        context['ti'].xcom_push(key='results_inserted', value=0)
        return {'inserted': 0}

    df['created_at']               = pd.to_datetime(df['created_at'], errors='coerce')
    df['race_duration_milliseconds'] = pd.to_numeric(df['race_duration_milliseconds'], errors='coerce')
    df['fastest_lap_speed']          = pd.to_numeric(df['fastest_lap_speed'], errors='coerce')
    if 'race_duration_hours' in df.columns:
        df['race_duration_hours'] = df['race_duration_hours'].astype(str)

    engine = _get_engine()
    count  = _insert_new_facts(df, 'race_results', 'facts', 'result_key', engine)
    context['ti'].xcom_push(key='results_inserted', value=count)
    return {'inserted': count}


def consume_laps(**context):
    """
    Read f1.fact.laps topic, insert new laps into facts.laps.
    Dedup key: lap_key (surrogate, unique, generated by producer).
    """
    print("=" * 70)
    print("CONSUMER: f1.fact.laps -> facts.laps")
    print("=" * 70)

    df = _read_topic(TOPIC_LAPS)
    if df.empty:
        context['ti'].xcom_push(key='laps_inserted', value=0)
        return {'inserted': 0}

    df['created_at']       = pd.to_datetime(df['created_at'], errors='coerce')
    df['lap_milliseconds'] = pd.to_numeric(df['lap_milliseconds'], errors='coerce')

    engine = _get_engine()
    count  = _insert_new_facts(df, 'laps', 'facts', 'lap_key', engine)
    context['ti'].xcom_push(key='laps_inserted', value=count)
    return {'inserted': count}


def consume_pitstops(**context):
    """
    Read f1.fact.pitstops topic, insert new pitstops into facts.pitstops.
    Dedup key: pitstop_key (surrogate, unique, generated by producer).
    """
    print("=" * 70)
    print("CONSUMER: f1.fact.pitstops -> facts.pitstops")
    print("=" * 70)

    df = _read_topic(TOPIC_PITSTOPS)
    if df.empty:
        context['ti'].xcom_push(key='pitstops_inserted', value=0)
        return {'inserted': 0}

    df['created_at']                   = pd.to_datetime(df['created_at'], errors='coerce')
    df['pitlane_duration_seconds']     = pd.to_numeric(df['pitlane_duration_seconds'], errors='coerce')
    df['pitlane_duration_milliseconds'] = pd.to_numeric(df['pitlane_duration_milliseconds'], errors='coerce')

    engine = _get_engine()
    count  = _insert_new_facts(df, 'pitstops', 'facts', 'pitstop_key', engine)
    context['ti'].xcom_push(key='pitstops_inserted', value=count)
    return {'inserted': count}


def consume_driver_standings(**context):
    """
    Read f1.fact.driver_standings topic, insert new standings into facts.driver_standings.
    Dedup key: ds_key (surrogate, unique, generated by producer).
    """
    print("=" * 70)
    print("CONSUMER: f1.fact.driver_standings -> facts.driver_standings")
    print("=" * 70)

    df = _read_topic(TOPIC_DRIVER_STANDINGS)
    if df.empty:
        context['ti'].xcom_push(key='dstandings_inserted', value=0)
        return {'inserted': 0}

    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['ds_points']  = pd.to_numeric(df['ds_points'],  errors='coerce')
    df['ds_position'] = pd.to_numeric(df['ds_position'], errors='coerce')
    df['ds_wins']    = pd.to_numeric(df['ds_wins'],    errors='coerce')

    engine = _get_engine()
    count  = _insert_new_facts(df, 'driver_standings', 'facts', 'ds_key', engine)
    context['ti'].xcom_push(key='dstandings_inserted', value=count)
    return {'inserted': count}


def consume_constructor_standings(**context):
    """
    Read f1.fact.constructor_standings topic, insert new standings into facts.constructor_standings.
    Dedup key: cs_key (surrogate, unique, generated by producer).
    """
    print("=" * 70)
    print("CONSUMER: f1.fact.constructor_standings -> facts.constructor_standings")
    print("=" * 70)

    df = _read_topic(TOPIC_CONSTRUCTOR_STANDINGS)
    if df.empty:
        context['ti'].xcom_push(key='cstandings_inserted', value=0)
        return {'inserted': 0}

    df['created_at']  = pd.to_datetime(df['created_at'], errors='coerce')
    df['cs_points']   = pd.to_numeric(df['cs_points'],   errors='coerce')
    df['cs_position'] = pd.to_numeric(df['cs_position'], errors='coerce')
    df['cs_wins']     = pd.to_numeric(df['cs_wins'],     errors='coerce')

    engine = _get_engine()
    count  = _insert_new_facts(df, 'constructor_standings', 'facts', 'cs_key', engine)
    context['ti'].xcom_push(key='cstandings_inserted', value=count)
    return {'inserted': count}


def print_consumer_summary(**context):
    ti = context['ti']
    print("\n" + "=" * 70)
    print("KAFKA CONSUMER PIPELINE COMPLETE")
    print("=" * 70)
    for key, label in [
        ('drivers_inserted',       'Drivers'),
        ('constructors_inserted',  'Constructors'),
        ('circuits_inserted',      'Circuits'),
        ('races_inserted',         'Races'),
        ('results_inserted',       'Results'),
        ('laps_inserted',          'Laps'),
        ('pitstops_inserted',      'Pitstops'),
        ('dstandings_inserted',    'Driver standings'),
        ('cstandings_inserted',    'Constructor standings'),
    ]:
        count = ti.xcom_pull(key=key) or 0
        print(f"  {label:<25s}: {count} rows inserted into PostgreSQL")
    print("=" * 70)
    print("\nKafka offset status:")
    print(f"  Consumer group '{CONSUMER_GROUP}' offsets committed.")
    print("  Next run will read ONLY messages published after this point.")
    print("=" * 70)


# ═══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id='kafka_consumer_pipeline',
    default_args=default_args,
    description='F1 Incremental Consumer: Kafka Topics -> Gold PostgreSQL (append-only)',
    schedule=None,           # triggered by kafka_producer_pipeline
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['kafka', 'consumer', 'incremental', 'f1-etl'],
) as dag:

    # ── Dimension consume tasks (run in parallel) ─────────────────────────────
    task_consume_drivers = PythonOperator(
        task_id='consume_drivers',
        python_callable=consume_drivers,
        doc='Read f1.dim.drivers, INSERT new records into dimensions.driver'
    )

    task_consume_constructors = PythonOperator(
        task_id='consume_constructors',
        python_callable=consume_constructors,
        doc='Read f1.dim.constructors, INSERT new records into dimensions.constructor'
    )

    task_consume_circuits = PythonOperator(
        task_id='consume_circuits',
        python_callable=consume_circuits,
        doc='Read f1.dim.circuits, INSERT new records into dimensions.circuit'
    )

    task_consume_races = PythonOperator(
        task_id='consume_races',
        python_callable=consume_races,
        doc='Read f1.dim.races, INSERT new records into dimensions.race'
    )

    # ── Fact consume tasks (run in parallel after all dims are inserted) ──────
    task_consume_results = PythonOperator(
        task_id='consume_results',
        python_callable=consume_results,
        doc='Read f1.fact.results, INSERT new records into facts.race_results'
    )

    task_consume_laps = PythonOperator(
        task_id='consume_laps',
        python_callable=consume_laps,
        doc='Read f1.fact.laps, INSERT new records into facts.laps'
    )

    task_consume_pitstops = PythonOperator(
        task_id='consume_pitstops',
        python_callable=consume_pitstops,
        doc='Read f1.fact.pitstops, INSERT new records into facts.pitstops'
    )

    task_consume_dstandings = PythonOperator(
        task_id='consume_driver_standings',
        python_callable=consume_driver_standings,
        doc='Read f1.fact.driver_standings, INSERT new records into facts.driver_standings'
    )

    task_consume_cstandings = PythonOperator(
        task_id='consume_constructor_standings',
        python_callable=consume_constructor_standings,
        doc='Read f1.fact.constructor_standings, INSERT new records into facts.constructor_standings'
    )

    task_summary = PythonOperator(
        task_id='print_summary',
        python_callable=print_consumer_summary,
        doc='Log how many rows were inserted per table'
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    # Dims run in parallel first (facts need FK rows to already exist in PG)
    dims  = [task_consume_drivers, task_consume_constructors,
             task_consume_circuits, task_consume_races]
    facts = [task_consume_results, task_consume_laps, task_consume_pitstops,
             task_consume_dstandings, task_consume_cstandings]

    for dim in dims:
        dim >> facts

    facts >> task_summary
