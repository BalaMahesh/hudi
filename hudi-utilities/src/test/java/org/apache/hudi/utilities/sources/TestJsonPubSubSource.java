/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Objects;
import java.util.UUID;

import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class TestJsonPubSubSource extends SparkClientFunctionalTestHarness {

  private static final URL SCHEMA_FILE_URL = TestJsonPubSubSource.class.getClassLoader().getResource("delta-streamer-config/source.avsc");
  private final HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);
  private FilebasedSchemaProvider schemaProvider;

  @BeforeEach
  public void init() throws Exception {
    String schemaFilePath = Objects.requireNonNull(SCHEMA_FILE_URL).toURI().getPath();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    schemaProvider = new FilebasedSchemaProvider(props, jsc());
  }

  private TypedProperties createPropsForJsonSource(String projectId, String subscriptionId, Long maxEventsToReadFromSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.pubsub.projectId", projectId);
    props.setProperty("hoodie.deltastreamer.source.pubsub.subscriptionId", subscriptionId);
    props.setProperty("auto.offset.reset", resetStrategy);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.setProperty("hoodie.deltastreamer.pubsub.source.maxEvents",
        maxEventsToReadFromSource != null ? String.valueOf(maxEventsToReadFromSource) :
            String.valueOf(100));
    return props;
  }

  @Test
  public void testJsonPubSubSource() {

    final String projectId = "glancecdn-sandbox-c32a";
    final String subscriptionId = "projects/glancecdn-sandbox-c32a/subscriptions/glance_online_unseen_sub";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(projectId, subscriptionId,null, "earliest");

    Source jsonSource = new JsonPubSubSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter pubSubSource = new SourceFormatAdapter(jsonSource);

    InputBatch<JavaRDD<GenericRecord>> fetch1 = pubSubSource.fetchNewDataInAvroFormat(Option.empty(), 300);

  }

}
