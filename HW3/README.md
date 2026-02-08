### SQL QUERIES FOR HW3

#### Creating tables
```sql 
CREATE OR REPLACE EXTERNAL TABLE `terraform-485601.nyc_trips.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_sreeja_b1/yellow_tripdata_2024-*.parquet']
);
 ```

```sql 
CREATE OR REPLACE TABLE `terraform-485601.nyc_trips.yellow_tripdata_non_partitioned_regular` AS
SELECT * FROM `terraform-485601.nyc_trips.external_yellow_tripdata`;
 ```
```sql 
CREATE OR REPLACE TABLE `terraform-485601.nyc_trips.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `terraform-485601.nyc_trips.external_yellow_tripdata`;
 ```


#### Q1 
```sql 
SELECT count(*) from terraform-485601.nyc_trips.external_yellow_tripdata;
 ```

#### Q2
```sql 
SELECT COUNT(DISTINCT PULocationID) FROM terraform-485601.nyc_trips.external_yellow_tripdata;

SELECT COUNT(DISTINCT PULocationID) FROM terraform-485601.nyc_trips.yellow_tripdata_non_partitioned_regular;
  ```

#### Q3
```sql 
SELECT PULocationID from terraform-485601.nyc_trips.yellow_tripdata_non_partitioned_regular;
SELECT PULocationID,DOLocationID from terraform-485601.nyc_trips.yellow_tripdata_non_partitioned_regular;
 ```
#### Q4
```sql 
select count(*) from terraform-485601.nyc_trips.yellow_tripdata_non_partitioned_regular where fare_amount = 0;
 ```
#### Q6
```sql 
SELECT DISTINCT VendorID
FROM terraform-485601.nyc_trips.yellow_tripdata_non_partitioned_regular
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';


SELECT DISTINCT VendorID
FROM terraform-485601.nyc_trips.yellow_tripdata_partitioned_clustered
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
 ```

#### Q9
```sql 
SELECT count(*) from terraform-485601.nyc_trips.yellow_tripdata_non_partitioned_regular;
 ```
