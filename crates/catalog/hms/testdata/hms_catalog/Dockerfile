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

FROM openjdk:8u342-jre

ENV HADOOP_VERSION=3.3.5
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

ENV HIVE_VERSION=3.1.3
ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
ENV PATH=$HIVE_HOME/bin:$PATH

# Set classpath for S3 Access
ENV HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.316.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.3.5.jar

WORKDIR /opt

RUN apt-get update && apt-get install -y procps fastjar

RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
  tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
  rm hadoop-${HADOOP_VERSION}.tar.gz

RUN wget https://downloads.apache.org/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
  tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz && \
  rm apache-hive-${HIVE_VERSION}-bin.tar.gz

RUN cd ${HIVE_HOME}/lib && \
  wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar

COPY ./hive-site.xml ${HIVE_HOME}/conf/hive-site.xml
COPY ./entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]