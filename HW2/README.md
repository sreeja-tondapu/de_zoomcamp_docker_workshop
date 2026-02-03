## HOMEWORK 2 SQL QUERIES

### 3. 
 ```sql
 SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) AS pickup_year,COUNT(*) as rows 
 FROM public.yellow_tripdata 
 GROUP BY 1;
 ```
 




 ### 4. 
 ```sql
 SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) AS pickup_year,EXTRACT(MONTH FROM tpep_pickup_datetime) AS pickup_month,COUNT(*) as rows 
 FROM public.yellow_tripdata 
 WHERE tpep_pickup_datetime >= '2021-03-01'
 GROUP BY 1,2;
  ```

  ### 5. 
  ##### I only inserted 2020 data and just did count(*) from public.green_tripdata to get the row count for 2020
