"""
One-off script: re-publish api_incremental Silver records to Kafka.

Reason: previous pipeline run saved data to Silver but failed on Kafka
(ECONNREFUSED). Now that Kafka is fixed, re-send those records.

Run from host machine:  python republish_to_kafka.py
"""

from minio import Minio
from kafka import KafkaProducer
import pandas as pd
import json
import io

# Host-side addresses (not Docker internal)
MINIO_ENDPOINT = 'localhost:9000'
KAFKA_BOOTSTRAP = 'localhost:9092'
SILVER_BUCKET = 'f1-silver'

TOPICS = {
    'drivers':      ('f1.dim.drivers',      'dimensions/dim_driver/dim_driver.parquet',           'driver_ref'),
    'constructors': ('f1.dim.constructors',  'dimensions/dim_constructor/dim_constructor.parquet', 'constructor_ref'),
    'circuits':     ('f1.dim.circuits',      'dimensions/dim_circuit/dim_circuit.parquet',         'circuit_references'),
    'races':        ('f1.dim.races',         'dimensions/dim_race/dim_race.parquet',               'race_id'),
}


def get_incremental_records(client: Minio, path: str) -> pd.DataFrame:
    data = client.get_object(SILVER_BUCKET, path)
    df = pd.read_parquet(io.BytesIO(data.read()))
    return df[df['source'] == 'api_incremental'].copy()


def publish(producer: KafkaProducer, topic: str, records: list, key_field: str):
    for row in records:
        key = str(row[key_field]) if key_field in row and row[key_field] is not None else None
        producer.send(topic, value=row, key=key)
    producer.flush()


def main():
    print("Connecting to MinIO and Kafka...")
    minio = Minio(MINIO_ENDPOINT, access_key='minioadmin', secret_key='minioadmin', secure=False)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None,
    )
    print("Connected.\n")

    for label, (topic, path, key_field) in TOPICS.items():
        try:
            df = get_incremental_records(minio, path)
            if df.empty:
                print(f"[{label}] 0 api_incremental records — skip")
                continue

            # Warn about NaN keys in races
            nan_keys = df[key_field].isna().sum() if key_field in df.columns else 0
            if nan_keys:
                print(f"[{label}] WARNING: {nan_keys} rows have NaN '{key_field}' — will be sent with null key")

            records = df.to_dict(orient='records')
            publish(producer, topic, records, key_field)
            print(f"[{label}] Published {len(records)} records to '{topic}'")

        except Exception as e:
            print(f"[{label}] ERROR: {e}")

    producer.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
