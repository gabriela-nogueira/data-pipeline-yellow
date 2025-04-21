from datetime import datetime
import sys
import boto3
import dotenv

dotenv.load_dotenv()

def create_table_or_partition(dat_ref):
    dat_formatted = datetime.strptime(dat_ref, '%Y-%m').strftime("%Y%m")
    client = boto3.client('athena', region_name='us-east-1')

    query = "CREATE DATABASE IF NOT EXISTS s_prd"
    output_location = 's3://prd-resultados-athena-gsn/athena/'

    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': output_location}
    )

    query = """
        CREATE EXTERNAL TABLE IF NOT EXISTS s_prd.yellow_taxi_trip (
        VendorID INT,
        passenger_count DOUBLE,
        total_amount DOUBLE,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP
    )
    PARTITIONED BY (dat_ref STRING)
    STORED AS PARQUET
    LOCATION 's3://prd-yellow-taxi-table-gabriela/'
    TBLPROPERTIES ('parquet.compress'='SNAPPY');
    """

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 's_prd'
        },
        ResultConfiguration={
            'OutputLocation': 's3://prd-resultados-athena-gsn/athena/',
        }
    )

    query_partition = f"""
    ALTER TABLE s_prd.yellow_taxi_trip
    ADD IF NOT EXISTS PARTITION (dat_ref='{dat_formatted}')
    LOCATION 's3://prd-yellow-taxi-table-gabriela/dat_ref={dat_formatted}/';
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