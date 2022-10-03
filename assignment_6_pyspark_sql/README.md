# **Assignment 6 Data Fellowship IYKRA**

---

# Pyspark and Spark SQL

### Objective:

Download the February 2021 data from TLC Trip Record website
(https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and use Pyspark to analyze and
answer these questions below. Upload your script into Github or Gdrive.
1. How many taxi trips were there on February 15?
2. Find the longest trip for each day ?
3. Find Top 5 Most frequent `dispatching_base_num` ?
4. Find Top 5 Most common location pairs (PUlocationID and DOlocationID) ?
5. Write all of the result to BigQuery table (additional - point plus)

---

### Answer

**1. How many taxi trips were there on February 15?**

Query:

`   SELECT
      
        *
    
    FROM 

    fhvhv_2_2021
    
    WHERE
    
    to_date(pickup_datetime) = '2021-02-15';``


![table for trips in 15 Feb 2021](/img/Screenshot%20from%202022-10-03%2021-49-27.png)

The total trips in Feb 15 2021 is:

![total trips Feb 2021](/img/Screenshot%20from%202022-10-03%2021-49-50.png)



**2. Find the longest trip for each day ?**

Query:

    `SELECT`
        `pickup_datetime, dropoff_datetime,`
        `(unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) AS duration_seconds`
    `FROM`
        `fhvhv_2_2021`
    `SORT BY`
        `duration_seconds DESC`


![longest trip table](/img/Screenshot%20from%202022-10-03%2021-50-09.png)

    The longest trip is 505361 seconds.



**3. Find Top 5 Most frequent `dispatching_base_num` ?**

Query: 


    `SELECT`
          `dispatching_base_num,`
          `COUNT(1) as count_dispatching_base_num`
    `FROM` 
          `fhvhv_2_2021`
    `GROUP BY`
          `1`
    `ORDER BY`
          `2 DESC`
    `LIMIT` 
          `5`

![most frequent](/img/Screenshot%20from%202022-10-03%2021-50-30.png)


**4. Find Top 5 Most common location pairs (PUlocationID and DOlocationID)?**

Query:

    `SELECT`
        `CONCAT(coalesce(PUzone, 'Unknown'), '/', coalesce(DOzone, 'Unknown')) AS zone_pair,`
        `COUNT(1) as total_count`
    `FROM`
    `zones_table_lookup`
    `GROUP BY`
        `1`
    `ORDER BY`
        `2 DESC`
    `LIMIT`
        `5`


![pair locations](/img/Screenshot%20from%202022-10-03%2021-50-45.png)


**Write all of the result to BigQuery table**

    pair_location.write.format('com.google.cloud.spark.bigquery') \
        .option('table', 'nyc_taxi_record.pair_location') \
        .save()