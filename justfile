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

default:
    just --list --unsorted

# start the performance-testing docker-compose project.
perf-start:
    docker-compose -p iceberg-rust-performance  --project-directory ./crates/iceberg/testdata/performance up -d
    # initialize the perf test if the environment is clean
    if [ ! -d ./crates/iceberg/testdata/performance/warehouse/warehouse/nyc/taxis/data ]; then just perf-init; fi

# stop the performance-testing docker-compose project
perf-stop:
    docker-compose -p iceberg-rust-performance  --project-directory ./crates/iceberg/testdata/performance down

# performance testing: initialize the tables and data
perf-init: perf-download-data
    docker exec iceberg-rust-performance-spark-iceberg-1 spark-sql --driver-memory 4G --executor-memory 4G -f /home/iceberg/spark_scripts/setup.sql

# stop and clean up the performance-testing docker-compose project docker images and data
perf-clean:
    docker-compose -p iceberg-rust-performance  --project-directory ./crates/iceberg/testdata/performance down --remove-orphans -v --rmi all
    rm ./crates/iceberg/testdata/performance/raw_data/*.parquet
    rm ./crates/iceberg/testdata/performance/warehouse/catalog.sqlite
    rm -rf ./crates/iceberg/testdata/performance/warehouse/warehouse
    rm -rf ./crates/iceberg/testdata/performance/warehouse/.minio.sys

# run the performance test suite
perf-run:
    # start the perf testing env if it is not running
    docker container inspect iceberg-rust-performance-rest-1 >/dev/null || just perf-start
    cargo criterion --benches

# download the "NYC Taxi" data required to populate the performance test suite
# Original links to these files can be found at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
perf-download-data:
    if [ ! -f ./crates/iceberg/testdata/performance/raw_data/yellow_tripdata_2024-01.parquet ]; then wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet -P crates/iceberg/testdata/performance/raw_data; fi
    if [ ! -f ./crates/iceberg/testdata/performance/raw_data/yellow_tripdata_2024-02.parquet ]; then wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet -P crates/iceberg/testdata/performance/raw_data; fi
    if [ ! -f ./crates/iceberg/testdata/performance/raw_data/yellow_tripdata_2024-03.parquet ]; then wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet -P crates/iceberg/testdata/performance/raw_data; fi
