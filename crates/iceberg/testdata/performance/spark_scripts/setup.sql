-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at

--   http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

SET TIME ZONE 'UTC';

CREATE DATABASE IF NOT EXISTS nyc.taxis;
CREATE TABLE IF NOT EXISTS nyc.taxis (
    VendorID              bigint,
    tpep_pickup_datetime  timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count       double,
    trip_distance         double,
    RatecodeID            double,
    store_and_fwd_flag    string,
    PULocationID          bigint,
    DOLocationID          bigint,
    payment_type          bigint,
    fare_amount           double,
    extra                 double,
    mta_tax               double,
    tip_amount            double,
    tolls_amount          double,
    improvement_surcharge double,
    total_amount          double,
    congestion_surcharge  double,
    airport_fee           double
)
USING iceberg
PARTITIONED BY (days(tpep_pickup_datetime));


ALTER TABLE nyc.taxis SET TBLPROPERTIES (
    'write.parquet.row-group-size-bytes'='131072',
    'write.parquet.page-row-limit'='200'
);
ALTER TABLE nyc.taxis WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY fare_amount;

CREATE TEMPORARY VIEW parquetTable
USING org.apache.spark.sql.parquet
OPTIONS (
  path "/home/iceberg/raw_data/yellow_tripdata_*.parquet"
);

-- Repeat the insert to accumulate multiple snapshots and manifest files
INSERT INTO nyc.taxis SELECT * FROM parquetTable;
--INSERT INTO nyc.taxis SELECT * FROM parquetTable;
--INSERT INTO nyc.taxis SELECT * FROM parquetTable;
