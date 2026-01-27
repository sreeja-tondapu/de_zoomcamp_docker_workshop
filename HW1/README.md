## HOMEWORK 1

SQL queries for questions 3 to 6 

Q3) 
```sql
SELECT  count(*) 
FROM public.green_taxi_2025_11
WHERE lpep_pickup_datetime >= '2025-11-01 00:00:00' 
  AND lpep_pickup_datetime < '2025-12-01 00:00:00'
  AND trip_distance <= 1.0;
```

Q4)
```sql
SELECT CAST(lpep_pickup_datetime AS DATE) AS pickup_day, MAX(trip_distance) AS longest_trip
FROM public.green_taxi_2025_11
WHERE trip_distance < 100
GROUP BY  pickup_day
ORDER BY longest_trip DESC
LIMIT 1;
```




Q5)
```sql
SELECT z."Zone", SUM(t."total_amount") AS total_amount_sum
FROM public.green_taxi_2025_11 t
JOIN public.taxi_zones z ON t."PULocationID" = z."LocationID"
WHERE t."lpep_pickup_datetime" >= '2025-11-18 00:00:00' 
    AND t."lpep_pickup_datetime" < '2025-11-19 00:00:00'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;
```

Q6)
```sql
SELECT zd."Zone" AS dropoff_zone,t."tip_amount"
FROM  public.green_taxi_2025_11 t
JOIN public.taxi_zones zp ON t."PULocationID" = zp."LocationID"
JOIN public.taxi_zones zd ON t."DOLocationID" = zd."LocationID"
WHERE zp."Zone" = 'East Harlem North'
ORDER BY t."tip_amount" DESC
LIMIT 1;
```


