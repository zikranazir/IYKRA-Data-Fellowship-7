# -*- coding: utf-8 -*-
"""assignment_6_pyspark_sql.ipynb"""

# Install wget use `pip install wget`
import wget

### Install Pyspark
"""

!pip install -q findspark

!pip install pyspark

import findspark

findspark.init()

"""### Iniate Spark Session and Import Library"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .master('local') \
    .appName('assignment_6') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

spark.version

"""### Connect To Bigquery

"""

from google.colab import auth
auth.authenticate_user()
print('Authenticated')

project_id = "data-fellowship-de"
from google.cloud import bigquery
import humanize
client = bigquery.Client(project=project_id)

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "iykra-fellowship" # change with your bucket name
spark.conf.set('temporaryGcsBucket', bucket)

"""### Download Dataset"""

!wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2021-02.parquet

"""Create Table Schema"""

from pyspark.sql import types

schema = types.StructType(
    [
        types.StructField('hvfhs_license_num', types.StringType(), True),
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.DoubleType(), True),
        types.StructField('DOLocationID', types.DoubleType(), True),
        types.StructField('SR_Flag', types.IntegerType(), True)
    ]
)

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .parquet('/content/fhv_tripdata_2021-02.parquet')

df.show()

"""Repartition it to 24 partitions."""

df= df.repartition(24)

"""### How many taxi in trips where in the 15 February?

#### Method 1
"""

from pyspark.sql import functions as F

trips_15_feb = df \
                    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
                    .filter("pickup_date = '2021-02-15'")      

trips_15_feb.show()         
print(f"total taxi trips in 15 February 2022: {trips_15_feb.count()} trips")

"""#### Method 2"""

df.registerTempTable('fhvhv_2_2021')

trips_15_feb = spark.sql("""
SELECT
    *
FROM 
    fhvhv_2_2021
WHERE
    to_date(pickup_datetime) = '2021-02-15';
""").show()

spark.sql("""
SELECT
    COUNT(1) as trips_15_feb
FROM 
    fhvhv_2_2021
WHERE
    to_date(pickup_datetime) = '2021-02-15';
""").show()

"""Send to Bigquery"""

trips_15_feb.write.format('com.google.cloud.spark.bigquery') \
  .option('table', 'nyc_taxi_record.trips_15_feb') \
  .save()

"""### Find Longest Time Each Day?

Calculate the duration for each trip

#### Method 1
"""

from pyspark.sql.functions import col, asc, desc

df \
    .withColumn('duration_hours', df.dropoff_datetime-df.pickup_datetime) \
    .orderBy(col('duration_hours').desc()) \
    .show()

"""#### Method 2"""

trips_duration = spark.sql("""
                              SELECT
                                  pickup_datetime, dropoff_datetime,
                                  (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) AS duration_seconds
                              FROM
                                  fhvhv_2_2021
                              SORT BY
                                  duration_seconds DESC
                              """)

trips_duration.show()

"""Send To Bigquery"""

trips_duration.write.format('com.google.cloud.spark.bigquery') \
  .option('table', 'nyc_taxi_record.trips_duration') \
  .save()

"""### Most frequent dispatching_base_num

Find the most frequently occurring dispatching_base_num in this dataset.

#### Method 1
"""

df \
    .groupBy('dispatching_base_num') \
    .count() \
    .orderBy('count', ascending=False) \
    .limit(5) \
    .show()

"""#### Method 2"""

most_dispatching_base_num = spark.sql("""
    SELECT 
          dispatching_base_num,
          COUNT(1) as count_dispatching_base_num
    FROM 
          fhvhv_2_2021
    GROUP BY
          1
    ORDER BY
          2 DESC
    LIMIT 
          5
""")

most_dispatching_base_num.show()

"""Send to BigQuery"""

most_dispatching_base_num.write.format('com.google.cloud.spark.bigquery') \
  .option('table', 'nyc_taxi_record.most_dispatching_base_num') \
  .save()

"""### 5 Most common location pairs

Find the most common pickup-dropoff pair.
"""

# Import Zone Lookup dataset

!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

# Create schema dataset
zones_schema = types.StructType([
    types.StructField('LocationID', types.IntegerType(), True),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True)
])

df_zones = spark.read.option('header', 'true').schema(zones_schema).csv('/content/taxi+_zone_lookup.csv')

df.printSchema()

df_zones.printSchema()

# create schema for zones pick up table
zpu = df_zones \
    .withColumnRenamed('Zone', 'PUzone') \
    .withColumnRenamed('LocationID', 'zPULocationID') \
    .withColumnRenamed('Borough', 'PUBorough') \
    .drop('service_zone')

# create schema for zones drop down table
zdo = df_zones \
    .withColumnRenamed('Zone', 'DOzone') \
    .withColumnRenamed('LocationID', 'zDOLocationID') \
    .withColumnRenamed('Borough', 'DOBorough') \
    .drop('service_zone')

# Join the table
df_join_temp = df.join(zpu, df.PULocationID == zpu.zPULocationID)
df_join = df_join_temp.join(zdo, df_join_temp.DOLocationID == zdo.zDOLocationID)

df_join.drop('PULocationID', 'DOLocationID', 'zPULocationID', 'zDOLocationID').write.parquet('zones')

df_join = spark.read.parquet('zones')
df_join.printSchema()

df_join.registerTempTable('zones_table_lookup')

pair_location = spark.sql("""
SELECT
    CONCAT(coalesce(PUzone, 'Unknown'), '/', coalesce(DOzone, 'Unknown')) AS zone_pair,
    COUNT(1) as total_count
FROM
    zones_table_lookup
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT
    5
;
""")

pair_location.show()

"""Send to Bigquery"""

pair_location.write.format('com.google.cloud.spark.bigquery') \
  .option('table', 'nyc_taxi_record.pair_location') \
  .save()