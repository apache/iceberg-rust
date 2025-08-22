#!/bin/bash
#
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
#

set -ex

SPARK_VERSION="3.5.6"
ICEBERG_VERSION="1.9.2"

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$ICEBERG_VERSION"
PACKAGES="$PACKAGES,org.apache.iceberg:iceberg-aws-bundle:$ICEBERG_VERSION"
PACKAGES="$PACKAGES,org.apache.spark:spark-connect_2.12:$SPARK_VERSION"

/opt/spark/sbin/start-connect-server.sh  \
  --packages $PACKAGES \
  --master local[3] \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:///spark-script/log4j2.properties" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalog.default=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.default.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.default.uri=http://rest:8181 \
  --conf spark.sql.catalog.default.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.default.s3.path.style.access=true \
  --conf spark.sql.catalog.default.s3.access.key=admin \
  --conf spark.sql.catalog.default.s3.secret.key=password \
  --conf spark.sql.defaultCatalog=default

tail -f /opt/spark/logs/spark*.out
