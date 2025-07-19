# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
import time

print("doing stuff...")


# Configure Spark for better performance with large datasets
spark = (
    SparkSession
        .builder
        .appName("IcebergDemo")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        # .config("spark.sql.catalog.rest.type", "rest")
        # .config("spark.sql.catalog.rest.uri", "http://rest:8181")
        # .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # .config("spark.sql.catalog.rest.warehouse", "s3://icebergdata/demo")
        # .config("spark.sql.catalog.rest.s3.endpoint", "http://minio:9000")
        # .config("spark.sql.defaultCatalog", "rest")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
)

# Set log level to reduce noise
spark.sparkContext.setLogLevel("INFO")

spark.sql(
    f"""
CREATE OR REPLACE TABLE rest.default.test_positional_merge_on_read_deletes (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
"""
)

spark.sql(
    f"""
INSERT INTO rest.default.test_positional_merge_on_read_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_deletes WHERE number = 9")

spark.sql(
    f"""
  CREATE OR REPLACE TABLE rest.default.test_positional_merge_on_read_double_deletes (
    dt     date,
    number integer,
    letter string
  )
  USING iceberg
  TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
  );
"""
)

spark.sql(
    f"""
INSERT INTO rest.default.test_positional_merge_on_read_double_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

#  Creates two positional deletes that should be merged
spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE number = 9")
spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE letter == 'f'")

#  Create a table, and do some renaming
spark.sql("CREATE OR REPLACE TABLE rest.default.test_rename_column (lang string) USING iceberg")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Python')")
spark.sql("ALTER TABLE rest.default.test_rename_column RENAME COLUMN lang TO language")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Java')")

#  Create a table, and do some evolution
spark.sql("CREATE OR REPLACE TABLE rest.default.test_promote_column (foo int) USING iceberg")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (19)")
spark.sql("ALTER TABLE rest.default.test_promote_column ALTER COLUMN foo TYPE bigint")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (25)")

#  Create a table, and do some evolution on a partition column
spark.sql("CREATE OR REPLACE TABLE rest.default.test_promote_partition_column (foo int, bar float, baz decimal(4, 2)) USING iceberg PARTITIONED BY (foo)")
spark.sql("INSERT INTO rest.default.test_promote_partition_column VALUES (19, 19.25, 19.25)")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN foo TYPE bigint")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN bar TYPE double")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN baz TYPE decimal(6, 2)")
spark.sql("INSERT INTO rest.default.test_promote_partition_column VALUES (25, 22.25, 22.25)")

#  Create a table with various types
spark.sql("""
CREATE OR REPLACE TABLE rest.default.types_test USING ICEBERG AS 
SELECT
    CAST(s % 2 = 1 AS BOOLEAN) AS cboolean,
    CAST(s % 256 - 128 AS TINYINT) AS ctinyint,
    CAST(s AS SMALLINT) AS csmallint,
    CAST(s AS INT) AS cint,
    CAST(s AS BIGINT) AS cbigint,
    CAST(s AS FLOAT) AS cfloat,
    CAST(s AS DOUBLE) AS cdouble,
    CAST(s / 100.0 AS DECIMAL(8, 2)) AS cdecimal,
    CAST(DATE('1970-01-01') + s AS DATE) AS cdate,
    CAST(from_unixtime(s) AS TIMESTAMP_NTZ) AS ctimestamp_ntz,
    CAST(from_unixtime(s) AS TIMESTAMP) AS ctimestamp,
    CAST(s AS STRING) AS cstring,
    CAST(s AS BINARY) AS cbinary
FROM (
    SELECT EXPLODE(SEQUENCE(0, 1000)) AS s
);
""")

print("Starting NYC Taxi Dataset Generation...")
start_time = time.time()

# Create the main taxi trips table
spark.sql(
    f"""
CREATE OR REPLACE TABLE rest.default.nyc_taxi_trips (
    trip_id STRING,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    payment_type STRING,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    total_amount DOUBLE,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    trip_type STRING,
    congestion_surcharge DOUBLE,
    pickup_date DATE
)
USING iceberg
PARTITIONED BY (pickup_date, vendor_id)
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2',
    'write.parquet.row-group-size-bytes'='33554432',
    'write.target-file-size-bytes'='134217728',
    'write.parquet.page-row-limit'='20000'
)
"""
)

print("Table created. Generating fake taxi trip data...")

# Generate data in batches to avoid memory issues
BATCH_SIZE = 100_000
TOTAL_ROWS = 10_000_000
NUM_BATCHES = TOTAL_ROWS // BATCH_SIZE

# NYC coordinates boundaries (approximate)
NYC_LAT_MIN, NYC_LAT_MAX = 40.477399, 40.917577
NYC_LON_MIN, NYC_LON_MAX = -74.259090, -73.700272

# Payment types
PAYMENT_TYPES = ["Credit Card", "Cash", "No Charge", "Dispute", "Unknown"]
TRIP_TYPES = ["Street-hail", "Dispatch"]

# Create UDF for generating random coordinates within NYC bounds
def generate_nyc_coordinates():
    lat = random.uniform(NYC_LAT_MIN, NYC_LAT_MAX)
    lon = random.uniform(NYC_LON_MIN, NYC_LON_MAX)
    return (lat, lon)

# Register UDF
spark.udf.register("generate_coords", generate_nyc_coordinates, ArrayType(DoubleType()))

for batch in range(NUM_BATCHES):
    print(f"Processing batch {batch + 1}/{NUM_BATCHES}...")

    # Generate base sequence for this batch
    batch_df = spark.range(BATCH_SIZE).select(
        # Trip ID
        concat(lit("trip_"), col("id").cast("string")).alias("trip_id"),

        # Vendor ID (1 or 2)
        (col("id") % 2 + 1).cast("integer").alias("vendor_id"),

        # Pickup datetime (last 2 years)
        (to_timestamp(lit("2022-01-01")) +
         expr("INTERVAL {} SECONDS".format(random.randint(0, 730 * 24 * 60 * 60)))).alias("pickup_datetime"),

        # Passenger count (1-6, weighted towards 1-2)
        when(col("id") % 100 < 60, 1)
        .when(col("id") % 100 < 85, 2)
        .when(col("id") % 100 < 95, 3)
        .when(col("id") % 100 < 98, 4)
        .when(col("id") % 100 < 99, 5)
        .otherwise(6).alias("passenger_count"),

        # Trip distance (0.1 to 30 miles, log-normal distribution simulation)
        round(
            when(col("id") % 1000 < 600, rand() * 3 + 0.5)  # Short trips
            .when(col("id") % 1000 < 900, rand() * 8 + 2)   # Medium trips
            .otherwise(rand() * 20 + 5),                     # Long trips
            2
        ).alias("trip_distance"),

        # Location IDs (1-265 for taxi zones)
        (col("id") % 265 + 1).cast("integer").alias("pickup_location_id"),
        ((col("id") + 17) % 265 + 1).cast("integer").alias("dropoff_location_id"),

        # Payment type
        when(col("id") % 100 < 70, "Credit Card")
        .when(col("id") % 100 < 95, "Cash")
        .when(col("id") % 100 < 98, "No Charge")
        .when(col("id") % 100 < 99, "Dispute")
        .otherwise("Unknown").alias("payment_type"),

        # Trip type
        when(col("id") % 10 < 8, "Street-hail")
        .otherwise("Dispatch").alias("trip_type")
    ).withColumn(
        # Generate pickup coordinates
        "pickup_coords", expr("array(rand() * {} + {}, rand() * {} + {})".format(
            NYC_LAT_MAX - NYC_LAT_MIN, NYC_LAT_MIN,
            NYC_LON_MAX - NYC_LON_MIN, NYC_LON_MIN
        ))
    ).withColumn(
        # Generate dropoff coordinates
        "dropoff_coords", expr("array(rand() * {} + {}, rand() * {} + {})".format(
            NYC_LAT_MAX - NYC_LAT_MIN, NYC_LAT_MIN,
            NYC_LON_MAX - NYC_LON_MIN, NYC_LON_MIN
        ))
    ).withColumn(
        # Calculate trip duration in seconds
        "trip_duration_seconds",
        (col("trip_distance") * 120 + 300 + (rand() * 600)).cast("integer")
    ).withColumn(
        # Calculate dropoff time (pickup + trip duration)
        "dropoff_datetime",
        col("pickup_datetime") + expr("make_interval(0, 0, 0, 0, 0, 0, trip_duration_seconds)")
    ).select(
        col("trip_id"),
        col("vendor_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        col("pickup_coords")[0].alias("pickup_latitude"),
        col("pickup_coords")[1].alias("pickup_longitude"),
        col("dropoff_coords")[0].alias("dropoff_latitude"),
        col("dropoff_coords")[1].alias("dropoff_longitude"),
        col("payment_type"),
        col("pickup_location_id"),
        col("dropoff_location_id"),
        col("trip_type"),
    ).withColumn(
    # Calculate fare based on distance and time
    "fare_amount",
    round(lit(2.50) + col("trip_distance") * lit(2.50) + rand() * lit(5.0), 2)
).withColumn(
    # Extra charges
    "extra",
    when(hour(col("pickup_datetime")).between(20, 23) |
         hour(col("pickup_datetime")).between(0, 5), lit(0.5))
    .otherwise(lit(0.0))
).withColumn(
    # MTA tax
    "mta_tax", lit(0.5)
).withColumn(
    # Tip amount (higher for credit cards)
    "tip_amount",
    when(col("payment_type") == "Credit Card",
         round(col("fare_amount") * (rand() * lit(0.25) + lit(0.15)), 2))
    .otherwise(lit(0.0))
).withColumn(
    # Tolls (random, sparse)
    "tolls_amount",
    when(rand() < lit(0.05), round(rand() * lit(8.0) + lit(2.0), 2))
    .otherwise(lit(0.0))
).withColumn(
    # Congestion surcharge (weekdays in Manhattan)
    "congestion_surcharge",
    when(col("pickup_location_id") <= 68, lit(2.5))  # Manhattan-ish zones
    .otherwise(lit(0.0))
).withColumn(
        # Total amount
        "total_amount",
        round(col("fare_amount") + col("extra") + col("mta_tax") +
              col("tip_amount") + col("tolls_amount") + col("congestion_surcharge"), 2)
    ).withColumn(
        # Pickup date for partitioning
        "pickup_date",
        to_date(col("pickup_datetime"))
    )

    batch_df = batch_df.select(
        col("trip_id").cast("string"),
        col("vendor_id").cast("integer"),
        col("pickup_datetime").cast("timestamp"),
        col("dropoff_datetime").cast("timestamp"),
        col("passenger_count").cast("integer"),
        col("trip_distance").cast("double"),
        col("pickup_longitude").cast("double"),
        col("pickup_latitude").cast("double"),
        col("dropoff_longitude").cast("double"),
        col("dropoff_latitude").cast("double"),
        col("payment_type").cast("string"),
        col("fare_amount").cast("double"),
        col("extra").cast("double"),
        col("mta_tax").cast("double"),
        col("tip_amount").cast("double"),
        col("tolls_amount").cast("double"),
        col("total_amount").cast("double"),
        col("pickup_location_id").cast("integer"),
        col("dropoff_location_id").cast("integer"),
        col("trip_type").cast("string"),
        col("congestion_surcharge").cast("double"),
        col("pickup_date").cast("date")
    )

    # Insert the batch
    batch_df.write.mode("append").insertInto("rest.default.nyc_taxi_trips")

    # Progress update
    rows_processed = (batch + 1) * BATCH_SIZE
    elapsed_time = time.time() - start_time
    rate = rows_processed / elapsed_time if elapsed_time > 0 else 0
    print(f"Inserted {rows_processed:,} rows. Rate: {rate:,.0f} rows/sec")

print(f"\nCompleted! Generated {TOTAL_ROWS:,} taxi trip records. Rewriting data files to apply sort")

print("Checking available system procedures...")

try:
    # Check if we can see system procedures
    result = spark.sql("SHOW PROCEDURES").collect()
    print("Available procedures:")
    for row in result:
        print(f"  {row}")
except Exception as e:
    print(f"Could not show procedures: {e}")

try:
    # Try to show procedures from the rest catalog specifically
    result = spark.sql("SHOW PROCEDURES rest.system").collect()
    print("Rest catalog procedures:")
    for row in result:
        print(f"  {row}")
except Exception as e:
    print(f"Could not show rest.system procedures: {e}")

# Enable more verbose logging to see what's happening
spark.sparkContext.setLogLevel("DEBUG")

print("Attempting rewrite with more verbose error handling...")

try:
    result = spark.sql("""
        CALL rest.system.rewrite_data_files(
            'default.nyc_taxi_trips',
            'sort',
            'zorder(pickup_latitude, pickup_longitude)'
        )
    """)
    print("Rewrite successful!")
    result.show()
except Exception as e:
    print(f"Detailed error: {e}")
    import traceback
    traceback.print_exc()

# Try alternative syntax variations
alternative_calls = [
    "CALL system.rewrite_data_files('default.nyc_taxi_trips')",
    "CALL rest.system.rewrite_data_files('default.nyc_taxi_trips')",
    "CALL rest.system.rewrite_data_files(table => 'default.nyc_taxi_trips')",
]

for call_sql in alternative_calls:
    try:
        print(f"Trying: {call_sql}")
        result = spark.sql(call_sql)
        print("Success!")
        result.show()
        break
    except Exception as e:
        print(f"Failed: {e}")

print(f"Total time: {time.time() - start_time:.2f} seconds")


spark.stop()