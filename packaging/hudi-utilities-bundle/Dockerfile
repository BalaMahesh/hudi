# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing,
#  * software distributed under the License is distributed on an
#  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  * KIND, either express or implied.  See the License for the
#  * specific language governing permissions and limitations
#  * under the License.
#  */

FROM apache/spark:v3.1.3
USER root
RUN  rm -rf /opt/spark/jars/guava-14.0.1.jar
RUN rm -rf  /opt/spark/jars/protobuf-java-2.5.0.jar
COPY api-common-2.1.3.jar /opt/spark/jars/
COPY gcs-connector-hadoop3-latest.jar  /opt/spark/jars/
COPY google-*  /opt/spark/jars/
COPY proto*   /opt/spark/jars/
COPY guava-27.0-jre.jar /opt/spark/jars/
COPY mysql-connector-java-8.0.22.jar /opt/spark/jars/
COPY perfmark-api-0.23.0.jar /opt/spark/jars/
COPY grpc* /opt/spark/jars/
COPY threetenbp-1.5.2.jar /opt/spark/jars/
COPY bijection* /opt/spark/jars/
COPY spark-avro_2.12-3.1.3.jar /opt/spark/jars/
COPY jmx_prometheus_javaagent-0.16.1.jar /opt/spark/jars/
COPY spark-streaming-kafka-0-10_2.12-3.2.1.jar /opt/spark/jars/
COPY spark-streaming-kafka-0-10-assembly_2.12-3.2.1.jar /opt/spark/jars/
COPY target/hudi-utilities-bundle_2.12-0.10.1.jar /opt/spark/jars/