#!/bin/sh

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

HIVE_VERSION=3.1.3
HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin

# Check if schema exists
${HIVE_HOME}/bin/schematool -dbType mysql -info

if [ $? -eq 1 ]; then
    echo "Getting schema info failed. Probably not initialized. Initializing...in 5s"
    sleep 5 
    ${HIVE_HOME}/bin/schematool -initSchema -dbType mysql
fi

${HIVE_HOME}/bin/hive --service metastore
