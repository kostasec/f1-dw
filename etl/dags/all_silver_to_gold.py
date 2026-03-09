from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.s3_helper import MinIOHelper


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SILVER_BUCKET = 'f1-silver'
POSTGRES_CONN_ID = 'postgres_f1_gold'


# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def get_postgres_engine():
    """Get PostgreSQL connection engine"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_sqlalchemy_engine()


def create_gold_schema(**context):
    """Drop and recreate Gold schemas in PostgreSQL"""
    print("=" * 60)
    print("DROP & CREATE: Gold schemas in PostgreSQL")
    print("=" * 60)
    
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # DROP existing schemas with all tables
    hook.run("""
        DROP SCHEMA IF EXISTS dimensions CASCADE;
        DROP SCHEMA IF EXISTS facts CASCADE;
    """)
    
    print("Dropped existing schemas")
    
    # CREATE fresh schemas
    hook.run("""
        CREATE SCHEMA dimensions;
        CREATE SCHEMA facts;
    """)
    
    print("Created fresh Gold schemas")
    return True


def format_value_or_unknown(val):
    """Convert to string or 'unknown' for TEXT fields only"""
    if pd.isna(val) or str(val) in ['None', 'NaT', 'nan', '', '<NA>', '\\N']:
        return 'unknown'
    return str(val)


# ═══════════════════════════════════════════════════════════════
# GOLD DIMENSIONS
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 1. DIM DRIVER
# ═══════════════════════════════════════════════════════════════
def transform_gold_dim_driver(**context):
    print("TRANSFORM: dim_driver -> Gold PostgreSQL")
    
    minio = MinIOHelper()
    df = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    
    # DATE columns - proper datetime conversion (NULL for invalid dates)
    df['driver_dob'] = pd.to_datetime(df['driver_dob'], errors='coerce')

    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

    # INTEGER columns - keep as nullable integer (was incorrectly converted to text)
    df['driver_number'] = pd.to_numeric(df['driver_number'], errors='coerce').astype('Int64')

    # TEXT columns with 'unknown' for NULL values
    df['driver_code'] = df['driver_code'].apply(format_value_or_unknown)
    df['driver_nationality'] = df['driver_nationality'].apply(format_value_or_unknown)
    df['driver_url'] = df['driver_url'].apply(format_value_or_unknown)

    # Column order
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

    df = df [column_order]
    print(f"Transformed {len(df)} drivers")
    
    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='driver',
        schema='dimensions',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded to PostgreSQL dimensions.driver")
    return len(df)

# ══════════════════════════════════════════════════════════════
# 2. DIM CONSTRUCTOR
# ═══════════════════════════════════════════════════════════════
def transform_gold_dim_constructor(**context):
    print("TRANSFORM: dim_constructor -> Gold PostgreSQL")
    
    minio = MinIOHelper()
    df = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')

    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

    # TEXT columns with 'unknown' for NULL values
    df['constructor_nationality'] = df['constructor_nationality'].apply(format_value_or_unknown)
    df['constructor_url'] = df['constructor_url'].apply(format_value_or_unknown)

    # Column order
    column_order=[
        'constructor_key',
        'constructor_id',
        'constructor_ref',
        'constructor_name',
        'constructor_nationality',
        'constructor_url',
        'created_at',
        'source'
    ]

    df = df [column_order]
    print(f"Transformed {len(df)} drivers")

    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='constructor',
        schema='dimensions',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} constructors to PostgreSQL")
    return len(df)

# ═══════════════════════════════════════════════════════════════
# 3. DIM CIRCUIT
# ═══════════════════════════════════════════════════════════════
def transform_gold_dim_circuit(**context):
    print("TRANSFORM: dim_circuit -> Gold PostgreSQL")
    
    minio = MinIOHelper()
    df = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_circuit/dim_circuit.parquet')
    
    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    
    # INTEGER columns - keep as nullable integer (was incorrectly converted to text)
    df['circuit_altitude'] = pd.to_numeric(df['circuit_altitude'], errors='coerce').astype('Int64')

    # Column order 
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

    df = df [column_order]
    print(f"Transformed {len(df)} drivers")
    
    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='circuit',
        schema='dimensions',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} circuits to PostgreSQL")
    return len(df)

# ═══════════════════════════════════════════════════════════════
# 4. DIM RACE
# ═══════════════════════════════════════════════════════════════
def transform_gold_dim_race(**context):
    print("TRANSFORM: dim_race -> Gold PostgreSQL")
    
    minio = MinIOHelper()
    df = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    
    # DATE columns - convert to proper datetime (NULL for invalid/missing dates)
    df['race_date'] = pd.to_datetime(df['race_date'], errors='coerce')
    df['fp1_date'] = pd.to_datetime(df['fp1_date'], errors='coerce')
    df['fp2_date'] = pd.to_datetime(df['fp2_date'], errors='coerce')
    df['fp3_date'] = pd.to_datetime(df['fp3_date'], errors='coerce')
    df['qualification_date'] = pd.to_datetime(df['qualification_date'], errors='coerce')
    df['sprint_date'] = pd.to_datetime(df['sprint_date'], errors='coerce')
    df['race_time'] = pd.to_datetime(df['race_time'], format='%H:%M:%S', errors='coerce').dt.time
    df['fp1_time'] = pd.to_datetime(df['fp1_time'], format='%H:%M:%S', errors='coerce').dt.time
    df['fp2_time'] = pd.to_datetime(df['fp2_time'], format='%H:%M:%S', errors='coerce').dt.time
    df['fp3_time'] = pd.to_datetime(df['fp3_time'], format='%H:%M:%S', errors='coerce').dt.time
    df['qualification_time'] = pd.to_datetime(df['qualification_time'], format='%H:%M:%S', errors='coerce').dt.time
    df['sprint_time'] = pd.to_datetime(df['sprint_time'], format='%H:%M:%S', errors='coerce').dt.time
    
    #TEXT columns with 'unknown' for NULL values
    df['race_url'] = df['race_url'].apply(format_value_or_unknown)

    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

   
    #Column order
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

    df = df [column_order]
    print(f"Transformed {len(df)} races")
    
    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='race',
        schema='dimensions',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} races to PostgreSQL")
    return len(df)

# ═══════════════════════════════════════════════════════════════
# 5. DIM TIME
# ═══════════════════════════════════════════════════════════════
def transform_gold_dim_time(**context):
    print("TRANSFORM: dim_time -> Gold PostgreSQL")
    
    minio = MinIOHelper()
    df = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_time/dim_time.parquet')
    
    
    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='time',
        schema='dimensions',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} time records to PostgreSQL")
    return len(df)


# ═══════════════════════════════════════════════════════════════
# GOLD FACTS
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 1. FACT RACE RESULTS
# ═══════════════════════════════════════════════════════════════
def transform_gold_fact_race_results(**context):
    print("TRANSFORM: fact_race_results -> Gold PostgreSQL")
    
    minio = MinIOHelper()
    
    # READ partitioned files
    all_results = []
    for year in range(2012, 2031):
        try:
            df_year = minio.read_parquet(SILVER_BUCKET, f'facts/fact_race_results/year={year}/data.parquet')
            all_results.append(df_year)
        except:
            pass
    
    df = pd.concat(all_results, ignore_index=True)
    
    
    # TEXT columns - keep as string
    df['race_duration_hours'] = df['race_duration_hours'].astype(str)
    
    # NUMERIC columns - convert to proper numeric types (NULL for invalid values)
    df['race_duration_milliseconds'] = pd.to_numeric(df['race_duration_milliseconds'], errors='coerce')
    df['fastest_lap_speed'] = pd.to_numeric(df['fastest_lap_speed'], errors='coerce')

    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

    # TEXT columns with 'unknown' for NULL values
    df['race_duration_hours'] = df['race_duration_hours'].apply(format_value_or_unknown)


    # Column order
    column_order=[
        'result_key',
        'result_id',
        'race_key',
        'driver_key',
        'constructor_key',
        'time_key',
        'year',
        'driver_number',
        'grid',
        'position',
        'position_text',
        'position_order',
        'points',
        'number_of_laps',
        'rank',
        'race_duration_hours',
        'race_duration_milliseconds',
        'fastest_lap',
        'fastest_lap_minutes',
        'fastest_lap_speed',
        'status',
        'created_at',
        'source'
    ]

    df = df [column_order]
    print(f"Transformed {len(df)} race results")
    
    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='race_results',
        schema='facts',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} race results to PostgreSQL")
    return len(df)

# ═══════════════════════════════════════════════════════════════════
# 2. FACT LAPS
# ═══════════════════════════════════════════════════════════════════
def transform_gold_fact_laps(**context):
    print("TRANSFORM: fact_laps -> Gold PostgreSQL")

    minio = MinIOHelper()

    # READ year-partitioned files (written by both CSV and API DAGs)
    all_laps = []
    for year in range(2012, 2031):
        try:
            df_year = minio.read_parquet(SILVER_BUCKET, f'facts/fact_laps/year={year}/data.parquet')
            all_laps.append(df_year)
        except:
            pass
    df = pd.concat(all_laps, ignore_index=True)
    
    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    
    # NUMERIC columns
    df['lap_milliseconds'] = pd.to_numeric(df['lap_milliseconds'], errors='coerce')
    
    print(f"Transformed {len(df)} laps")

    # Column order
    column_order=[
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
        'source'
    ]

    df = df [column_order]
    print(f"Transformed {len(df)} race results")
    
    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='laps',
        schema='facts',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )


    print(f"Loaded {len(df)} laps to PostgreSQL")
    return len(df)

# ═══════════════════════════════════════════════════════════════════
# 3. FACT PITSTOPS
# ═══════════════════════════════════════════════════════════════════
def transform_gold_fact_pitstops(**context):
    print("TRANSFORM: fact_pitstops -> Gold PostgreSQL")

    minio = MinIOHelper()

    # READ year-partitioned files (written by both CSV and API DAGs)
    all_pitstops = []
    for year in range(2012, 2031):
        try:
            df_year = minio.read_parquet(SILVER_BUCKET, f'facts/fact_pitstops/year={year}/data.parquet')
            all_pitstops.append(df_year)
        except:
            pass
    df = pd.concat(all_pitstops, ignore_index=True)
    
    # NUMERIC columns
    df['pitlane_duration_seconds'] = pd.to_numeric(df['pitlane_duration_seconds'], errors='coerce')
    df['pitlane_duration_milliseconds'] = pd.to_numeric(df['pitlane_duration_milliseconds'], errors='coerce')
    
    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    print(f"Transformed {len(df)} pitstops")

    # Column order
    column_order=[
        'pitstop_key',
        'race_key',
        'driver_key',
        'stop',
        'constructor_key',
        'time_key',
        'lap',
        'local_time',
        'pitlane_duration_seconds',
        'pitlane_duration_milliseconds',
        'created_at',
        'source'
    ]

    df = df [column_order]
    print(f"Transformed {len(df)} race results")
    
    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='pitstops',
        schema='facts',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} pitstops to PostgreSQL")
    return len(df)

# ═══════════════════════════════════════════════════════════════════
# 4. FACT DRIVER STANDINGS
# ═══════════════════════════════════════════════════════════════════
def transform_gold_fact_driver_standings(**context):
    print("TRANSFORM: fact_driver_standings -> Gold PostgreSQL")
    
    minio = MinIOHelper()
    df = minio.read_parquet(SILVER_BUCKET, 'facts/fact_driver_standings/fact_driver_standings.parquet')
    
    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    
    # NUMERIC columns
    df['ds_points'] = pd.to_numeric(df['ds_points'], errors='coerce')
    df['ds_position'] = pd.to_numeric(df['ds_position'], errors='coerce')
    df['ds_wins'] = pd.to_numeric(df['ds_wins'], errors='coerce')
    
    print(f"Transformed {len(df)} driver standings")

    # Column order 
    column_order = [
        'ds_key',
        'ds_id',
        'race_key',
        'driver_key',
        'constructor_key',
        'time_key',
        'ds_points',
        'ds_position',
        'ds_wins',
        'created_at',
        'source'
    ]

    df = df [column_order]
    df = df.sort_values(['race_key', 'driver_key']).reset_index(drop=True)
    print(f"Loaded {len(df)} driver standings")
    
    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='driver_standings',
        schema='facts',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} driver standings to PostgreSQL")
    return len(df)

# ═══════════════════════════════════════════════════════════════════
# 5. FACT CONSTRUCTOR STANDINGS
# ═══════════════════════════════════════════════════════════════════
def transform_gold_fact_constructor_standings(**context):
    print("TRANSFORM: fact_constructor_standings -> Gold PostgreSQL")
    
    minio = MinIOHelper()
    df = minio.read_parquet(SILVER_BUCKET, 'facts/fact_constructor_standings/fact_constructor_standings.parquet')

    # TIMESTAMP columns
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    
    # NUMERIC columns
    df['cs_points'] = pd.to_numeric(df['cs_points'], errors='coerce')
    df['cs_position'] = pd.to_numeric(df['cs_position'], errors='coerce')
    df['cs_wins'] = pd.to_numeric(df['cs_wins'], errors='coerce')

    # Column order
    column_order = [
        'cs_key',
        'cs_id',
        'race_key',
        'constructor_key',
        'time_key',
        'cs_points',
        'cs_position',
        'cs_wins',
        'created_at',
        'source'
    ]

    df = df [column_order]
    df = df.sort_values(['race_key', 'constructor_key']).reset_index(drop=True)
    print(f"Transformed {len(df)} race results")

    # WRITE to PostgreSQL
    engine = get_postgres_engine()
    df.to_sql(
        name='constructor_standings',
        schema='facts',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} constructor standings to PostgreSQL")
    return len(df)


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id='all_silver_to_gold',
    default_args=default_args,
    description='F1 ETL: Silver (MinIO Parquet) -> Gold (PostgreSQL with proper data types)',
    schedule=None,
    start_date=datetime(2026, 2, 3),
    catchup=False,
    tags=['silver', 'dims&facts', 'postgresql', 'csv&api'],
) as dag:
    
    # ═══════════════════════════════════════════════════════════════
    # SETUP
    # ═══════════════════════════════════════════════════════════════
    create_schema = PythonOperator(
        task_id='create_gold_schema',
        python_callable=create_gold_schema
    )
    
    # ═══════════════════════════════════════════════════════════════
    # DIMENSIONS
    # ═══════════════════════════════════════════════════════════════
    gold_dim_driver = PythonOperator(
        task_id='transform_gold_dim_driver',
        python_callable=transform_gold_dim_driver
    )
    
    gold_dim_constructor = PythonOperator(
        task_id='transform_gold_dim_constructor',
        python_callable=transform_gold_dim_constructor
    )
    
    gold_dim_circuit = PythonOperator(
        task_id='transform_gold_dim_circuit',
        python_callable=transform_gold_dim_circuit
    )
    
    gold_dim_race = PythonOperator(
        task_id='transform_gold_dim_race',
        python_callable=transform_gold_dim_race
    )
    
    gold_dim_time = PythonOperator(
        task_id='transform_gold_dim_time',
        python_callable=transform_gold_dim_time
    )
    
    # ═══════════════════════════════════════════════════════════════
    # FACTS
    # ═══════════════════════════════════════════════════════════════
    gold_fact_results = PythonOperator(
        task_id='transform_gold_fact_race_results',
        python_callable=transform_gold_fact_race_results
    )
    
    gold_fact_laps = PythonOperator(
        task_id='transform_gold_fact_laps',
        python_callable=transform_gold_fact_laps
    )
    
    gold_fact_pitstops = PythonOperator(
        task_id='transform_gold_fact_pitstops',
        python_callable=transform_gold_fact_pitstops
    )
    
    gold_fact_driver_standings = PythonOperator(
        task_id='transform_gold_fact_driver_standings',
        python_callable=transform_gold_fact_driver_standings
    )
    
    gold_fact_constructor_standings = PythonOperator(
        task_id='transform_gold_fact_constructor_standings',
        python_callable=transform_gold_fact_constructor_standings
    )
    
    # ═══════════════════════════════════════════════════════════════
    # DEPENDENCIES
    # ═══════════════════════════════════════════════════════════════
    
    create_schema >> [gold_dim_driver, gold_dim_constructor, gold_dim_circuit, gold_dim_race, gold_dim_time]
    
    [gold_dim_driver, gold_dim_constructor, gold_dim_race, gold_dim_time] >> gold_fact_results
    [gold_dim_driver, gold_dim_constructor, gold_dim_race, gold_dim_time] >> gold_fact_laps
    [gold_dim_driver, gold_dim_constructor, gold_dim_race, gold_dim_time] >> gold_fact_pitstops
    [gold_dim_driver, gold_dim_constructor, gold_dim_race, gold_dim_time] >> gold_fact_driver_standings
    [gold_dim_constructor, gold_dim_race, gold_dim_time] >> gold_fact_constructor_standings