from datetime import datetime
import sys
import boto3
import dotenv

dotenv.load_dotenv()

def create_table_or_partition(db, table_name, athena_s3, table_bucket, data_ref, schema):
    client = boto3.client('athena', region_name='us-east-1')

    query = f"CREATE DATABASE IF NOT EXISTS {db}"
    output_location = f's3://{athena_s3}'

    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': output_location}
    )

    columns = ", ".join(f"{col} {tipo}" for col, tipo in schema.items() if col != 'data_ref')

    query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table_name} (
        {columns}
    )
    PARTITIONED BY (data_ref STRING)
    STORED AS PARQUET
    LOCATION 's3://{table_bucket}/'
    TBLPROPERTIES ('parquet.compress'='SNAPPY');
    """

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': db
        },
        ResultConfiguration={
            'OutputLocation': output_location,
        }
    )

    query_partition = f"""
    ALTER TABLE {db}.{table_name}
    ADD IF NOT EXISTS PARTITION (data_ref='{data_ref}')
    LOCATION 's3://{table_bucket}/data_ref={data_ref}/';
    """

    response = client.start_query_execution(
        QueryString=query_partition,
        ResultConfiguration={'OutputLocation': output_location}
    )

    print("QueryExecutionId:", response['QueryExecutionId'])

    return None

if __name__ == '__main__':
    dat_ref = sys.argv[1]
    create_table_or_partition(dat_ref)