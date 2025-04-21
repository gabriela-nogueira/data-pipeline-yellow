CREATE EXTERNAL TABLE IF NOT EXISTS prd.yellow_taxi_trip (
    VendorID STRING,
    passenger_count DOUBLE,
    total_amount DOUBLE,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP
)
PARTITIONED BY (dat_ref STRING)
STORED AS PARQUET
LOCATION 's3://prd-yellow-taxi-table-gabriela/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');