from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
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

BRONZE_CSV = '/opt/airflow/data/bronze/f1_data.csv'
BRONZE_BUCKET = 'f1-bronze'
SILVER_BUCKET = 'f1-silver'


def extract_bronze(**context):
    print("=" * 60)
    print("EXTRACT: Reading Bronze CSV and uploading to MinIO")
    print("=" * 60)
    
    # Učitaj CSV sa low_memory=False
    print("STEP 1: Reading CSV with low_memory=False...")
    df = pd.read_csv(BRONZE_CSV, low_memory=False, dtype={'alt': str})
    print(f"STEP 2: Loaded {len(df):,} rows from CSV")
    
    # STEP 3: Fix problematic columns BEFORE PyArrow sees them
    print("STEP 3: Cleaning data types...")
    
    # Fix 'alt' column specifically
    if 'alt' in df.columns:
        df['alt'] = pd.to_numeric(df['alt'], errors='coerce')
        print(f"  Fixed 'alt' column: {df['alt'].dtype}")
    
    # Fix all object columns that should be numeric
    for col in df.select_dtypes(include=['object']).columns:
        try:
            temp = pd.to_numeric(df[col], errors='coerce')
            if temp.notna().sum() / len(df) > 0.5:
                df[col] = temp
                print(f"  Converted {col} to numeric")
        except:
            pass
    
    print("STEP 4: Initializing MinIO and creating buckets if needed...")
    minio = MinIOHelper()
    
    # Kreiraj bronze bucket ako ne postoji
    if not minio.client.bucket_exists(BRONZE_BUCKET):
        minio.client.make_bucket(BRONZE_BUCKET)
        print(f"  Created bucket: {BRONZE_BUCKET}")
    else:
        print(f"  Bucket {BRONZE_BUCKET} already exists")
    
    # Kreiraj silver bucket ako ne postoji
    if not minio.client.bucket_exists(SILVER_BUCKET):
        minio.client.make_bucket(SILVER_BUCKET)
        print(f"  Created bucket: {SILVER_BUCKET}")
    else:
        print(f"  Bucket {SILVER_BUCKET} already exists")
    
    print("STEP 5: Uploading to MinIO...")
    minio.upload_parquet(df, BRONZE_BUCKET, 'raw_data/f1_data.parquet')
    print("STEP 6: Upload complete!")
    
    context['ti'].xcom_push(key='total_rows', value=len(df))
    return len(df)

# ═══════════════════════════════════════════════════════════════
# SILVER DIMENSIONS
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 1. DIM DRIVER
# ═══════════════════════════════════════════════════════════════
def transform_dim_driver(**context):
    print("TRANSFORM: dim_driver → Silver (Parquet with surrogate key)")
    
    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')
    
    # Extract unique drivers
    drivers = df[[
        'driverId', 'driverRef', 'number_drivers', 'code',
        'forename', 'surname', 'dob', 'nationality', 'url'
    ]].drop_duplicates(subset=['driverId']).sort_values(["driverId"], ascending=True).copy()
    
    # Rename columns
    drivers = drivers.rename(columns={
        'driverId'      : 'driver_id',
        'driverRef'     : 'driver_ref',
        'number_drivers': 'driver_number',
        'code'          : 'driver_code',
        'forename'      : 'driver_forename',
        'surname'       : 'driver_surname',
        'dob'           : 'driver_dob',
        'nationality'   : 'driver_nationality',
        'url'           : 'driver_url'
    })
    
    # Format date column
    drivers['driver_dob'] = pd.to_datetime(drivers['driver_dob'], errors='coerce').dt.date
    
    # Format nullable integer
    drivers['driver_number'] = pd.to_numeric(drivers['driver_number'], errors='coerce').astype('Int64')
    
    # Generate SURROGATE KEY
    drivers = drivers.reset_index(drop=True)
    drivers.insert(0, 'driver_key', range(1, len(drivers) + 1))
    
    # Metadata
    drivers['created_at'] = pd.Timestamp.now()
    drivers['source'] = 'f1_data.csv'
    
    print(f"Generated {len(drivers)} drivers with surrogate keys (1-{len(drivers)})")

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

    drivers = drivers[column_order]
    print(f"Transformed {len(drivers)} drivers")
    
    
    # Upload to MinIO
    minio.upload_parquet(drivers, SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    
    return len(drivers)

# ═══════════════════════════════════════════════════════════════
# 2. DIM CONSTRUCTOR
# ═══════════════════════════════════════════════════════════════
def transform_dim_constructor(**context):
    print("TRANSFORM: dim_constructor → Silver (Parquet with surrogate key)")
    
    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')
    
    # Extract unique constructors
    constructors = df[[
        'constructorId', 'constructorRef', 'name',
        'nationality_constructors', 'url_constructors'
    ]].drop_duplicates(subset=['constructorId']).sort_values(["constructorId"], ascending=True).copy()
    
    # Rename columns
    constructors = constructors.rename(columns={
        'constructorId'           : 'constructor_id',
        'constructorRef'          : 'constructor_ref',
        'name'                    : 'constructor_name',
        'nationality_constructors': 'constructor_nationality',
        'url_constructors'        : 'constructor_url'
    })
    
    # Generate SURROGATE KEY
    constructors = constructors.reset_index(drop=True)
    constructors.insert(0, 'constructor_key', range(1, len(constructors) + 1))
    
    # Metadata
    constructors['created_at'] = pd.Timestamp.now()
    constructors['source'] = 'f1_data.csv'
    
    print(f"Generated {len(constructors)} constructors with surrogate keys")
    
    # Upload to MinIO
    minio.upload_parquet(constructors, SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    
    return len(constructors)

# ═══════════════════════════════════════════════════════════════
# 3. DIM CIRCUIT
# ═══════════════════════════════════════════════════════════════
def transform_dim_circuit(**context):
    print("TRANSFORM: dim_circuit → Silver (Parquet with surrogate key)")
    
    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')
    
    # Extract unique circuits
    circuits = df[[
        'circuitId', 'circuitRef', 'name_y', 'location',
        'country', 'lat', 'lng', 'alt', 'url_y'
    ]].drop_duplicates(subset=['circuitId']).sort_values(["circuitId"], ascending=True).copy()

    # Type conversion
    circuits['circuitId'] = circuits['circuitId'].astype('int64')
    circuits['lat'] = pd.to_numeric(circuits['lat'], errors='coerce')
    circuits['lng'] = pd.to_numeric(circuits['lng'], errors='coerce')
    circuits['alt'] = pd.to_numeric(circuits['alt'], errors='coerce').astype('Int64')
    
    # Rename columns
    circuits = circuits.rename(columns={
        'circuitId'     : 'circuit_id',
        'circuitRef'    : 'circuit_references',
        'name_y'        : 'circuit_name', 
        'location'      : 'circuit_city',
        'country'       : 'circuit_country',
        'lat'           : 'circuit_latitude',
        'lng'           : 'circuit_longtitude',
        'alt'           : 'circuit_altitude',
        'url_y'         : 'circuit_url',
    })

    # Generate SURROGATE KEY
    circuits = circuits.reset_index(drop=True)
    circuits.insert(0, 'circuit_key', range(1, len(circuits) + 1))
    
    # Metadata
    circuits['created_at'] = pd.Timestamp.now()
    circuits['source'] = 'f1_data.csv'
    
    print(f"Generated {len(circuits)} circuits with surrogate keys")
    
    # Upload to MinIO
    minio.upload_parquet(circuits, SILVER_BUCKET, 'dimensions/dim_circuit/dim_circuit.parquet')
    
    return len(circuits)

# ═══════════════════════════════════════════════════════════════
# 4. DIM RACE
# ═══════════════════════════════════════════════════════════════
def transform_dim_race(**context):
    print("TRANSFORM: dim_race → Silver (Parquet with surrogate key)")
    
    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')
    
    # Extract unique races
    races = df[[
        'raceId', 'circuitId', 'name_x', 'year', 'round', 'date', 'time_races', 'url_x',
        'fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time',
        'quali_date', 'quali_time', 'sprint_date', 'sprint_time'
    ]].drop_duplicates(subset=['raceId']).sort_values(["raceId"], ascending=True).copy()
    
    # Rename columns
    races = races.rename(columns={
        'raceId'        : 'race_id',
        'circuitId'     : 'circuit_id',
        'name_x'        : 'race_name',
        'year'          : 'race_year',
        'round'         : 'race_round',
        'date'          : 'race_date', 
        'time_races'    : 'race_time',
        'url_x'         : 'race_url',
        'fp1_date'      : 'fp1_date',
        'fp1_time'      : 'fp1_time',
        'fp2_date'      : 'fp2_date',
        'fp2_time'      : 'fp2_time',
        'fp3_date'      : 'fp3_date',
        'fp3_time'      : 'fp3_time',
        'quali_date'    : 'qualification_date',
        'quali_time'    : 'qualification_time',
        'sprint_date'   : 'sprint_date',
        'sprint_time'   : 'sprint_time'
    })

    # Format date/time columns
    races['race_date'] = pd.to_datetime(races['race_date'], errors='coerce').dt.date
    races['race_time'] = pd.to_datetime(races['race_time'], errors='coerce').dt.time
    races['fp1_date'] = pd.to_datetime(races['fp1_date'], errors='coerce').dt.date
    races['fp1_time'] = pd.to_datetime(races['fp1_time'], errors='coerce').dt.time
    races['fp2_date'] = pd.to_datetime(races['fp2_date'], errors='coerce').dt.date
    races['fp2_time'] = pd.to_datetime(races['fp2_time'], errors='coerce').dt.time
    races['fp3_date'] = pd.to_datetime(races['fp3_date'], errors='coerce').dt.date
    races['fp3_time'] = pd.to_datetime(races['fp3_time'], errors='coerce').dt.time
    races['qualification_date'] = pd.to_datetime(races['qualification_date'], errors='coerce').dt.date
    races['qualification_time'] = pd.to_datetime(races['qualification_time'], errors='coerce').dt.time
    races['sprint_date'] = pd.to_datetime(races['sprint_date'], errors='coerce').dt.date
    races['sprint_time'] = pd.to_datetime(races['sprint_time'], errors='coerce').dt.time
    
    # Generate SURROGATE KEY
    races = races.reset_index(drop=True)
    races.insert(0, 'race_key', range(1, len(races) + 1))
    
    # Load dim_circuit and map circuit_key
    dim_circuit = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_circuit/dim_circuit.parquet')
    circuit_map = dict(zip(dim_circuit['circuit_id'], dim_circuit['circuit_key']))
    races['circuit_key'] = races['circuit_id'].map(circuit_map)

    # Metadata
    races['created_at'] = pd.Timestamp.now()
    races['source'] = 'f1_data.csv'
    
    # Column order 
    column_order = [
        'race_key',
        'race_id',
        'circuit_id',
        'circuit_key',
        'race_name',
        'race_year',
        'race_round',
        'race_date', 
        'race_time',
        'race_url',
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
        'created_at',
        'source'
    ]

    races = races[column_order]
    print(f"Generated {len(races)} races with surrogate keys")
    
    # Upload
    minio.upload_parquet(races, SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    
    return len(races)

# ═══════════════════════════════════════════════════════════════
# 5. DIM TIME
# ═══════════════════════════════════════════════════════════════
def transform_dim_time(**context):
    print("TRANSFORM: dim_time → Silver (Parquet with sequential surrogate key)")
    
    # Generate date range
    dates = pd.date_range(start='2012-01-01', end='2030-12-31', freq='D')
    
    df = pd.DataFrame({'full_date': dates})
    
    # Generate SURROGATE KEY
    df.insert(0, 'time_key', range(1, len(df) + 1))
    
    # Extract components
    df['year'] = dates.year
    df['month'] = dates.month
    df['day_of_month'] = dates.day
    df['day_of_week'] = dates.dayofweek + 1
    df['day_name'] = dates.day_name()
    df['day_name_short'] = dates.strftime('%a')
    df['month_name'] = dates.month_name()
    df['quarter'] = 'Q' + dates.quarter.astype(str)
    df['week_of_month'] = ((dates.day - 1) // 7) + 1
    df['is_weekend'] = dates.dayofweek >= 5
    df['is_weekday'] = dates.dayofweek < 5
    
    
    print(f"Generated {len(df):,} time records with sequential surrogate keys (1-{len(df)})")
    
    minio = MinIOHelper()
    minio.upload_parquet(df, SILVER_BUCKET, 'dimensions/dim_time/dim_time.parquet')
    
    return len(df)

# ═══════════════════════════════════════════════════════════════
# SILVER FACTS
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 1. FACT RACE RESULTS
# ═══════════════════════════════════════════════════════════════
def transform_fact_race_results(**context):
    print("TRANSFORM: fact_race_results → Silver (Parquet with surrogate keys)")
    
    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')
    
    # Load dimensions
    dim_driver = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    dim_constructor = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    dim_race = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    dim_time = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_time/dim_time.parquet')
    
    # Create lookup dictionaries
    driver_map = dict(zip(dim_driver['driver_id'], dim_driver['driver_key']))
    constructor_map = dict(zip(dim_constructor['constructor_id'], dim_constructor['constructor_key']))
    race_map = dict(zip(dim_race['race_id'], dim_race['race_key']))
    
    # TIME KEY mapping (2-step: race_id → date → time_key)
    race_to_date = dict(zip(dim_race['race_id'], dim_race['race_date']))
    time_map = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))
    
    # Select & transform fact columns
    results = df[[
        'resultId', 'raceId', 'driverId', 'constructorId', 'number',
        'grid', 'position', 'positionText', 'positionOrder', 'points', 'laps',
        'time', 'milliseconds', 'fastestLap', 'rank', 'fastestLapTime',
        'fastestLapSpeed', 'status', 'year'
    ]].drop_duplicates(subset=['resultId']).sort_values(["resultId"], ascending = True).copy()

    #Type conversion
    results['resultId'] = results['resultId'].astype('int64')
    results['raceId'] = results['raceId'].astype('int64')
    results['driverId'] = results['driverId'].astype('int64')
    results['constructorId'] = results['constructorId'].astype('int64')
    results['number'] = pd.to_numeric(results['number'], errors='coerce').fillna(0).astype('int64')
    results['grid'] = pd.to_numeric(results['grid'], errors='coerce').fillna(0).astype('int64')
    results['position'] = pd.to_numeric(results['position'], errors='coerce')
    results['positionOrder'] = pd.to_numeric(results['positionOrder'], errors='coerce').fillna(0).astype('int64')
    results['points'] = pd.to_numeric(results['points'], errors='coerce')
    results['laps'] = pd.to_numeric(results['laps'], errors='coerce').fillna(0).astype('int64')
    results['milliseconds'] = pd.to_numeric(results['milliseconds'], errors='coerce')
    results['fastestLap'] = pd.to_numeric(results['fastestLap'], errors='coerce')
    results['rank'] = pd.to_numeric(results['rank'], errors='coerce')
    results['fastestLapSpeed'] = pd.to_numeric(results['fastestLapSpeed'], errors='coerce')
    results['year'] = results['year'].astype('int64')
    results['positionText'] = results['positionText'].astype(str)

    # Rename columns
    results = results.rename(columns={
        'resultId'        :     'result_id',
        'number'          :      'driver_number',
        'positionText'    :     'position_text',
        'positionOrder'   :     'position_order',
        'points'          :     'points',
        'laps'            :     'number_of_laps',
        'time'            :     'race_duration_hours',
        'milliseconds'    :     'race_duration_milliseconds',
        'rank'            :     'rank',
        'fastestLap'      :     'fastest_lap',
        'fastestLapTime'  :     'fastest_lap_minutes',
        'fastestLapSpeed' :     'fastest_lap_speed',
        'status'          :     'status'
        })
    
    # Generate SURROGATE KEY for fact
    results = results.reset_index(drop=True)
    results.insert(0, 'result_key', range(1, len(results) + 1))
    
    # Map foreign keys to surrogate keys
    results['driver_key'] = results['driverId'].map(driver_map)
    results['constructor_key'] = results['constructorId'].map(constructor_map)
    results['race_key'] = results['raceId'].map(race_map)
    
    # Map time_key (2-step: raceId → race_date → time_key)
    results['race_date_temp'] = results['raceId'].map(race_to_date)
    results['time_key'] = results['race_date_temp'].map(time_map)
    
    # Drop natural keys and temp columns
    results = results.drop(columns=['driverId', 'raceId', 'constructorId', 'race_date_temp'])
    
    # Metada
    results['created_at'] = pd.Timestamp.now()
    results['source'] = 'f1_data.csv'
    
    print(f"Transformed {len(results):,} race results with surrogate keys")
    
    # Upload partitioned by year
    for year in sorted(results['year'].unique()):
        year_data = results[results['year'] == year]
        object_name = f'facts/fact_race_results/year={year}/data.parquet'
        minio.upload_parquet(year_data, SILVER_BUCKET, object_name)
        print(f"Year {year}: {len(year_data)} rows")
    
    return len(results)

# ═══════════════════════════════════════════════════════════════════
# 2. FACT LAPS
# ═══════════════════════════════════════════════════════════════════
def transform_fact_laps(**context):
    print("TRANSFORM: fact_laps → Silver (Parquet with surrogate key)")
    
    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')
    
    # Load dimensions
    dim_driver = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    dim_constructor = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    dim_race = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    dim_time = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_time/dim_time.parquet')
    
    #Create lookup dictionaries
    driver_map = dict(zip(dim_driver['driver_id'], dim_driver['driver_key']))
    constructor_map = dict(zip(dim_constructor['constructor_id'], dim_constructor['constructor_key']))
    race_map = dict(zip(dim_race['race_id'], dim_race['race_key']))
    race_to_date = dict(zip(dim_race['race_id'], dim_race['race_date']))
    time_map = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))
    
    # 3. Select & transform fact columns
    laps = df[['raceId', 'driverId', 'constructorId', 'lap', 'position_laptimes', 
           'time_laptimes', 'milliseconds_laptimes'
           ]].drop_duplicates().sort_values(["raceId", "lap", "driverId"], ascending=True).copy()

    # Rename columns
    laps = laps.rename(columns={
        'position_laptimes'     : 'lap_position',
        'time_laptimes'         : 'lap_minutes',
        'milliseconds_laptimes' : 'lap_milliseconds'
    })

    # Generate SURROGATE KEY
    laps = laps.reset_index(drop=True)
    laps.insert(0, 'lap_key', range(1, len(laps) + 1))
    
    #Map foreign keys to surrogate keys
    laps['driver_key'] = laps['driverId'].map(driver_map)
    laps['constructor_key'] = laps['constructorId'].map(constructor_map)
    laps['race_key'] = laps['raceId'].map(race_map)
    
    # Map time_key (2-step)
    laps['race_date_temp'] = laps['raceId'].map(race_to_date)
    laps['time_key'] = laps['race_date_temp'].map(time_map)

    # Derive year for partitioning
    race_year_map = dict(zip(dim_race['race_id'], dim_race['race_year']))
    laps['year'] = laps['raceId'].map(race_year_map)

    # Drop natural keys
    laps = laps.drop(columns=['driverId', 'constructorId', 'raceId', 'race_date_temp'])

    #Metadata
    laps['created_at'] = pd.Timestamp.now()
    laps['source'] = 'f1_data.csv'

    print(f"Transformed {len(laps)} laps with surrogate keys")

    # Write partitioned by year (consistent with results and API laps)
    for year in sorted(laps['year'].dropna().unique()):
        year_data = laps[laps['year'] == year]
        object_name = f'facts/fact_laps/year={int(year)}/data.parquet'
        minio.upload_parquet(year_data, SILVER_BUCKET, object_name)
        print(f"Year {int(year)}: {len(year_data)} rows")

    return len(laps)

# ═══════════════════════════════════════════════════════════════════
# 3. FACT PITSTOPS
# ═══════════════════════════════════════════════════════════════════
def transform_fact_pitstops(**context):
    print("TRANSFORM: fact_pitstops → Silver (Parquet with surrogate key)")
    
    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')
    
    # Load dimensions
    dim_driver = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    dim_constructor = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    dim_race = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    dim_time = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_time/dim_time.parquet')
    
    # Create lookup dictionaries
    driver_map = dict(zip(dim_driver['driver_id'], dim_driver['driver_key']))
    constructor_map = dict(zip(dim_constructor['constructor_id'], dim_constructor['constructor_key']))
    race_map = dict(zip(dim_race['race_id'], dim_race['race_key']))
    race_to_date = dict(zip(dim_race['race_id'], dim_race['race_date']))
    time_map = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))

    # Select & transform columns
    pitstops = df[['raceId', 'driverId', 'stop', 'constructorId', 'lap_pitstops',
                    'time_pitstops', 'duration', 'milliseconds_pitstops'
                    ]].drop_duplicates().sort_values(['raceId', 'driverId', 'stop'], ascending=True).copy()
    
    # Type conversion
    pitstops['raceId'] = pitstops['raceId'].astype('int64')
    pitstops['driverId'] = pitstops['driverId'].astype('int64')
    pitstops['stop'] = pd.to_numeric(pitstops['stop'], errors='coerce').fillna(0).astype('int64')
    pitstops['lap_pitstops'] = pd.to_numeric(pitstops['lap_pitstops'], errors='coerce').fillna(0).astype('int64')
    pitstops['duration'] = pd.to_numeric(pitstops['duration'], errors='coerce')
    pitstops['milliseconds_pitstops'] = pd.to_numeric(pitstops['milliseconds_pitstops'], errors='coerce').fillna(0).astype('int64')

    # Rename columns
    pitstops = pitstops.rename(columns={
        'lap_pitstops': 'lap',
        'time_pitstops': 'local_time',
        'duration' : 'pitlane_duration_seconds',
        'milliseconds_pitstops': 'pitlane_duration_milliseconds'
    })
    
    # Generate SURROGATE KEY
    pitstops = pitstops.reset_index(drop=True)
    pitstops.insert(0, 'pitstop_key', range(1, len(pitstops) + 1))
    
    # Map foreign keys to surrogate keys
    pitstops['driver_key'] = pitstops['driverId'].map(driver_map)
    pitstops['constructor_key'] = pitstops['constructorId'].map(constructor_map)
    pitstops['race_key'] = pitstops['raceId'].map(race_map)

    # Map time_key (2-step)
    pitstops['race_date_temp'] = pitstops['raceId'].map(race_to_date)
    pitstops['time_key'] = pitstops['race_date_temp'].map(time_map)

    # Derive year for partitioning
    race_year_map = dict(zip(dim_race['race_id'], dim_race['race_year']))
    pitstops['year'] = pitstops['raceId'].map(race_year_map)

    # Drop natural keys
    pitstops = pitstops.drop(columns=['driverId', 'constructorId', 'raceId', 'race_date_temp'])

    # Metadata
    pitstops['created_at'] = pd.Timestamp.now()
    pitstops['source'] = 'f1_data.csv'

    print(f"Generated {len(pitstops)} pitstops with surrogate keys")

    # Write partitioned by year (consistent with results and API pitstops)
    for year in sorted(pitstops['year'].dropna().unique()):
        year_data = pitstops[pitstops['year'] == year]
        object_name = f'facts/fact_pitstops/year={int(year)}/data.parquet'
        minio.upload_parquet(year_data, SILVER_BUCKET, object_name)
        print(f"Year {int(year)}: {len(year_data)} rows")

    return len(pitstops)

# ═══════════════════════════════════════════════════════════════════
# 4. FACT DRIVER STANDINGS
# ═══════════════════════════════════════════════════════════════════
def transform_fact_driver_standings(**context):
    print("TRANSFORM: fact_driver_standings → Silver (Parquet with surrogate keys)")

    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')

    # Load dimensions
    dim_race = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    dim_driver = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_driver/dim_driver.parquet')
    dim_constructor = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    dim_time = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_time/dim_time.parquet')

    # Create lookup dictionaries 
    driver_map = dict(zip(dim_driver['driver_id'], dim_driver['driver_key']))
    constructor_map = dict(zip(dim_constructor['constructor_id'], dim_constructor['constructor_key']))
    race_map = dict(zip(dim_race['race_id'], dim_race['race_key']))
    race_to_date = dict(zip(dim_race['race_id'], dim_race['race_date']))
    time_map = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))
    
    # Select & transform fact columns
    driver_standings = df[['raceId','driverId','driverStandingsId',
                           'constructorId','points_driverstandings', 'position_driverstandings','wins'
                           ]].drop_duplicates().sort_values(['driverStandingsId'], ascending=True).copy()
    
    # Rename columns
    driver_standings = driver_standings.rename(columns={
        'driverStandingsId' : 'ds_id',
        'points_driverstandings' : 'ds_points',
        'position_driverstandings' : 'ds_position',
        'wins' : 'ds_wins'
    })

    # Generate SURROGATE KEY
    driver_standings = driver_standings.reset_index(drop=True)
    driver_standings.insert(0, 'ds_key', range(1, len(driver_standings) +1))

    # Map foreign keys to surrogate keys
    driver_standings['driver_key'] = driver_standings['driverId'].map(driver_map)
    driver_standings['constructor_key'] = driver_standings['constructorId'].map(constructor_map)
    driver_standings['race_key'] = driver_standings['raceId'].map(race_map)

    # Map time_key (2-step)
    driver_standings['race_date_temp'] = driver_standings ['raceId'].map(race_to_date)
    driver_standings['time_key'] = driver_standings['race_date_temp'].map(time_map)

    # Drop natural keys
    driver_standings = driver_standings.drop(columns=['driverId','raceId','constructorId','race_date_temp'])
    
    # Metadata
    driver_standings['created_at'] = pd.Timestamp.now()
    driver_standings['source'] = 'f1_data.csv'

    minio.upload_parquet(driver_standings, SILVER_BUCKET, 'facts/fact_driver_standings/fact_driver_standings.parquet')

    return len(driver_standings)

# ═══════════════════════════════════════════════════════════════════
# 5. FACT CONSTRUCTOR STANDINGS
# ═══════════════════════════════════════════════════════════════════
def transform_fact_constructor_standings(**context):
    print("TRANSFORM: fact_constructor_standings → Silver (Parquet with surrogate keys)")
    
    minio = MinIOHelper()
    df = minio.read_parquet(BRONZE_BUCKET, 'raw_data/f1_data.parquet')

    # Load dimensions
    dim_race = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_race/dim_race.parquet')
    dim_constructor = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_constructor/dim_constructor.parquet')
    dim_time = minio.read_parquet(SILVER_BUCKET, 'dimensions/dim_time/dim_time.parquet')

    #Create lookup dictionaries
    constructor_map = dict(zip(dim_constructor['constructor_id'], dim_constructor['constructor_key']))
    race_map = dict(zip(dim_race['race_id'], dim_race['race_key']))
    race_to_date = dict(zip(dim_race['race_id'], dim_race['race_date']))
    time_map = dict(zip(pd.to_datetime(dim_time['full_date']).dt.date, dim_time['time_key']))
    
    #Select & transform fact columns
    constructor_standings = df[['raceId', 'constructorId', 'constructorStandingsId',
                                'points_constructorstandings', 'position_constructorstandings',
                                'wins_constructorstandings'
                                ]].drop_duplicates().sort_values(['constructorStandingsId'], ascending=True).copy()
    
    # Rename columns
    constructor_standings = constructor_standings.rename(columns={
        'constructorStandingsId' : 'cs_id',
        'points_constructorstandings': 'cs_points',
        'position_constructorstandings': 'cs_position',
        'wins_constructorstandings': 'cs_wins'
    })

    # Generate SURROGATE KEY
    constructor_standings = constructor_standings.reset_index(drop=True)
    constructor_standings.insert(0, 'cs_key', range(1, len(constructor_standings) + 1))

    # Map foreign keys to surrogate keys
    constructor_standings['constructor_key'] = constructor_standings['constructorId'].map(constructor_map)
    constructor_standings['race_key'] = constructor_standings['raceId'].map(race_map)

    # Map time_key (2-step)
    constructor_standings['race_date_temp'] = constructor_standings['raceId'].map(race_to_date)
    constructor_standings['time_key'] = constructor_standings['race_date_temp'].map(time_map)

    # Drop natural keys
    constructor_standings = constructor_standings.drop(columns=['constructorId', 'raceId', 'race_date_temp'])
    
    # Metadata
    constructor_standings['created_at'] = pd.Timestamp.now()
    constructor_standings['source'] = 'f1_data.csv'
    
    minio.upload_parquet(constructor_standings, SILVER_BUCKET, 'facts/fact_constructor_standings/fact_constructor_standings.parquet')
    return len(constructor_standings)

# ═══════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════
with DAG(
    dag_id='csv_bronze_to_silver',
    default_args=default_args,
    description='F1 ETL with Sequential Surrogate Keys: Bronze CSV → Silver Parquet (MinIO)',
    schedule=None,
    start_date=datetime(2026, 2, 2),
    catchup=False,
    tags=['silver', 'dims&facts', 'minio', 'csv'],
) as dag:
    
# ═══════════════════════════════════════════════════════════════
# EXTRACT
# ═══════════════════════════════════════════════════════════════
    extract = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze
    )
    
# ═══════════════════════════════════════════════════════════════
# DIMENSIONS
# ═══════════════════════════════════════════════════════════════
    transform_driver = PythonOperator(
        task_id='transform_dim_driver',
        python_callable=transform_dim_driver
    )
    
    transform_constructor = PythonOperator(
        task_id='transform_dim_constructor',
        python_callable=transform_dim_constructor
    )
    
    transform_circuit = PythonOperator(
        task_id='transform_dim_circuit',
        python_callable=transform_dim_circuit
    )
    
    transform_race = PythonOperator(
        task_id='transform_dim_race',
        python_callable=transform_dim_race
    )
    
    transform_time = PythonOperator(
        task_id='transform_dim_time',
        python_callable=transform_dim_time
    )
    
# ═══════════════════════════════════════════════════════════════
# FACTS
# ═══════════════════════════════════════════════════════════════
    transform_results = PythonOperator(
        task_id='transform_fact_race_results',
        python_callable=transform_fact_race_results
    )
    
    transform_laps = PythonOperator(
        task_id='transform_fact_laps',
        python_callable=transform_fact_laps
    )
    
    transform_pitstops = PythonOperator(
        task_id='transform_fact_pitstops',
        python_callable=transform_fact_pitstops
    )
    
    transform_driver_standings = PythonOperator(
        task_id='transform_fact_driver_standings',
        python_callable=transform_fact_driver_standings
    )

    transform_constructor_standings = PythonOperator(
        task_id='transform_fact_constructor_standings',
        python_callable=transform_fact_constructor_standings
    )
    
# ═══════════════════════════════════════════════════════════════
# DEPENDENCIES
# ═══════════════════════════════════════════════════════════════
    
    # 1. Extract → Independent Dimensions (parallel)
    extract >> [transform_driver, transform_constructor, transform_circuit, transform_time]

    # 1b. Circuit must finish before Race (transform_race reads dim_circuit to map circuit_key)
    transform_circuit >> transform_race

    # 2. Dimensions → fact_race_results
    [transform_driver, transform_constructor, transform_race, transform_time] >> transform_results

    # 3. Dimensions → fact_laps
    [transform_driver, transform_constructor, transform_race, transform_time] >> transform_laps

    # 4. Dimensions → fact_pitstops
    [transform_driver, transform_constructor, transform_race, transform_time] >> transform_pitstops

    # 5. Dimensions → fact_driver_standings
    [transform_driver, transform_constructor, transform_race, transform_time] >> transform_driver_standings

    # 6. Dimensions → fact_constructor_standings
    [transform_constructor, transform_race, transform_time] >> transform_constructor_standings