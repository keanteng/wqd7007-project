# Hive Reference

## Create Table and Load Data
You can create an external table in Hive to load your data. Below is an example of how to create a table for taxi trip data and perform some basic data cleaning operations.

```sql
-- Create an external table for the taxi data
CREATE EXTERNAL TABLE IF NOT EXISTS taxi_trips (
    VendorID INT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID INT,
    PULocationID INT,
    DOLocationID INT,
    passenger_count INT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee STRING,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type INT,
    trip_type INT,
    congestion_surcharge DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data2'
TBLPROPERTIES ('skip.header.line.count'='1');
```

To check the created table, you can use the following command on Hive:

```bash
# show tables
SHOW TABLES;

# describe the table
DESCRIBE taxi_trips;
```

## Data Cleaning and Transformation

```sql
CREATE TABLE taxi_trips_cleaned AS
SELECT 
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    -- Handle missing or empty store_and_fwd_flag values explicitly
    CASE 
        WHEN store_and_fwd_flag IS NULL OR store_and_fwd_flag = '' THEN 'N'
        ELSE store_and_fwd_flag
    END AS store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    -- Handle zero or null passenger counts
    CASE 
        WHEN passenger_count IS NULL OR passenger_count = 0 THEN 1
        ELSE passenger_count
    END AS passenger_count,
    -- Filter out unreasonable trip distances
    CASE
        WHEN trip_distance < 0 OR trip_distance > 100 THEN NULL
        ELSE trip_distance
    END AS trip_distance,
    -- Convert null monetary values to zero
    COALESCE(fare_amount, 0) AS fare_amount,
    COALESCE(extra, 0) AS extra,
    COALESCE(mta_tax, 0) AS mta_tax,
    COALESCE(tip_amount, 0) AS tip_amount,
    COALESCE(tolls_amount, 0) AS tolls_amount,
    -- Calculate trip duration in minutes
    (UNIX_TIMESTAMP(lpep_dropoff_datetime) - UNIX_TIMESTAMP(lpep_pickup_datetime)) / 60 AS trip_duration_minutes, 
    COALESCE(improvement_surcharge, 0) AS improvement_surcharge,
    COALESCE(total_amount, 0) AS total_amount,
    payment_type,
    trip_type,
    COALESCE(congestion_surcharge, 0) AS congestion_surcharge
FROM 
    taxi_trips
WHERE
    -- Filter out missing timestamps or invalid trip duration
    lpep_pickup_datetime IS NOT NULL AND
    lpep_dropoff_datetime IS NOT NULL AND
    lpep_dropoff_datetime > lpep_pickup_datetime AND

    -- Filter out negative values
    (fare_amount >= 0 OR fare_amount IS NULL) AND
    (extra >= 0 OR extra IS NULL) AND
    (mta_tax >= 0 OR mta_tax IS NULL) AND
    (tip_amount >= 0 OR tip_amount IS NULL) AND
    (tolls_amount >= 0 OR tolls_amount IS NULL) AND
    (improvement_surcharge >= 0 OR improvement_surcharge IS NULL) AND
    (total_amount >= 0 OR total_amount IS NULL) AND
    (congestion_surcharge >= 0 OR congestion_surcharge IS NULL);

-- Identify potential data quality issues
CREATE TABLE data_quality_issues AS
SELECT
    'Invalid pickup/dropoff time' AS issue_type,
    COUNT(*) AS record_count
FROM
    taxi_trips
WHERE
    lpep_dropoff_datetime <= lpep_pickup_datetime

UNION ALL
SELECT
    'Null pickup or dropoff datetime' AS issue_type,
    COUNT(*) AS record_count
FROM
    taxi_trips
WHERE
    lpep_pickup_datetime IS NULL OR lpep_dropoff_datetime IS NULL

UNION ALL
SELECT
    'Zero passengers' AS issue_type,
    COUNT(*) AS record_count
FROM
    taxi_trips
WHERE
    passenger_count = 0

UNION ALL
SELECT
    'Negative trip distance' AS issue_type,
    COUNT(*) AS record_count
FROM
    taxi_trips
WHERE
    trip_distance < 0

UNION ALL
SELECT
    'Negative fare amount' AS issue_type,
    COUNT(*) AS record_count
FROM
    taxi_trips
WHERE
    fare_amount < 0

UNION ALL
SELECT
    'Negative total amount' AS issue_type,
    COUNT(*) AS record_count
FROM
    taxi_trips
WHERE
    total_amount < 0;

-- Create a summary table with aggregated statistics by date
CREATE TABLE trip_daily_summary AS
SELECT
    TO_DATE(lpep_pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS avg_distance,
    MAX(trip_distance) AS max_distance,
    AVG((UNIX_TIMESTAMP(lpep_dropoff_datetime) - UNIX_TIMESTAMP(lpep_pickup_datetime)) / 60) AS avg_duration_minutes,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    SUM(total_amount) AS total_revenue,
    COUNT(DISTINCT VendorID) AS active_vendors
FROM
    taxi_trips_cleaned
GROUP BY
    TO_DATE(lpep_pickup_datetime);
```

![Alt](img1.png)

## Saving The Cleaned Data

> I recommend you to read this section

When you save data you data will not be saved together with the header so you need some tricks to save the header together with the data. You can use the following code on hive to save the data:

```sql
-- Export the data
INSERT OVERWRITE DIRECTORY '/data2/csv_data'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
SELECT * FROM taxi_trips_cleaned;

INSERT OVERWRITE DIRECTORY '/data2/csv_header'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
  'vendor_id',
  'pickup_time',
  'dropoff_time',
  'flag',
  'rate_code',
  'pu_location',
  'do_location',
  'passenger_count',
  'trip_distance',
  'fare_amount',
  'extra',
  'mta_tax',
  'tip_amount',
  'tolls_amount',
  'trip_duration_minutes',  -- ✅ this was missing
  'improvement_surcharge',
  'total_amount',
  'payment_type',
  'trip_type',
  'congestion_surcharge'
FROM taxi_trips_cleaned LIMIT 1;

![Alt](img3.png)

Then open another bash terminal and run the following code to merge the header and data together:

```bash
# make sure you are in the bash terminal of your container
docker exec -it hive-server bash
cat /data2/csv_header/000000_0 /data2/csv_data/000000_0 > /data2/final_with_headers.csv
``` 

![Alt](img4.png)
