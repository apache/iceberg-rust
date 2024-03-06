#!/bin/sh

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
