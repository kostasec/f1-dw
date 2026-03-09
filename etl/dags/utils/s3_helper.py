from minio import Minio
from minio.error import S3Error
import pyarrow.parquet as pq
import pyarrow as pa
import io
import pandas as pd

class MinIOHelper:
    def __init__(self, endpoint='minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
    
    def upload_parquet(self, df, bucket_name, object_name):
        """Upload pandas DataFrame as Parquet to MinIO"""
        try:
            # Convert DataFrame to Parquet
            parquet_buffer = io.BytesIO()
            table = pa.Table.from_pandas(df, preserve_index=False, safe=False)  # ← IZMENJENO
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)
            
            # Upload to MinIO
            self.client.put_object(
                bucket_name,
                object_name,
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            print(f'Uploaded {object_name} ({len(df)} rows)')
        except Exception as e:
            print(f'Error uploading {object_name}: {e}')
            raise
    
    def read_parquet(self, bucket_name, object_name):
        '''Download Parquet file from MinIO and return as pandas DataFrame'''
        try:
            # Download object from MinIO
            response = self.client.get_object(bucket_name, object_name)
            parquet_data = response.read()
            response.close()
            response.release_conn()
            
            # Convert to DataFrame
            parquet_buffer = io.BytesIO(parquet_data)
            df = pd.read_parquet(parquet_buffer)
            
            print(f' Downloaded {object_name} ({len(df)} rows)')
            return df
        except Exception as e:
            print(f'Error reading {object_name}: {e}')
            raise
    
    def list_objects(self, bucket_name, prefix=''):
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            print(f'Error: {e}')
            return []
