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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.ReceivedMessage;
import com.inmobi.glance.datatypes.analytics.GlancePublisherMessage;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class ProtoPubSubPullSource extends PubSubSource{

  private static final Logger LOG = LogManager.getLogger(ProtoPubSubPullSource.class);

  private Integer partitions = 8;

  public ProtoPubSubPullSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                               SchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider, metrics);
    partitions = Integer.parseInt(props.containsKey("hoodie.deltastreamer.source.pubsub.rdd.partitions") ?
        props.getString("hoodie.deltastreamer.source.pubsub.rdd.partitions") : "8");
    LOG.info("Created the pub sub source with rdd partitions:: "+partitions);
  }

  @Override
  protected InputBatch<JavaRDD<String>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    List<String> messages = new ArrayList<>();
    List<ReceivedMessage> receivedMessages = fetchNewMessages(lastCkptStr,sourceLimit);
    receivedMessages.forEach(receivedMessage -> {
      try {
        messages.add(Any.parseFrom(receivedMessage.getMessage().getData()).unpack(GlancePublisherMessage.class).getData());
      } catch (InvalidProtocolBufferException e) {
        LOG.error("Error parsing message from proto message:: {}",e);
      }
      ackIds.add(receivedMessage.getAckId());
    });
    LOG.info("Received messages list size :: "+messages.size());
    String checkPointStr = ackIds.size() > 0 ? ackIds.get(0)+"-"+ackIds.get(ackIds.size()-1) : "no-messages" ;
    JavaRDD<String> newDataRDD = sparkContext.parallelize(messages,partitions);
    return new InputBatch<>(Option.of(newDataRDD), checkPointStr);

  }

  @Override
  public void onCommit(String lastCkptStr) {
   super.onCommit(lastCkptStr);

  }

}
