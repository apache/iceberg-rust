# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM apache/hive:4.2.1

USER root

# Link the hadoop-aws and AWS SDK jars that the image ships into the metastore classpath
RUN ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar /opt/hive/lib/ && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/bundle-*.jar /opt/hive/lib/

# The entrypoint links this directory into the Hive config directory, over its own core-site.xml
ENV HIVE_CUSTOM_CONF_DIR=/opt/hive/custom-conf
COPY core-site.xml ${HIVE_CUSTOM_CONF_DIR}/core-site.xml

USER hive
