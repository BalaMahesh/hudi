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

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JsonPubSubSource extends JsonSource {

  private static final Logger LOG = LogManager.getLogger(JsonPubSubSource.class);

  private SubscriberStub subscriber;
  private PullRequest pullRequest;
  private String subscriptionId = "";
  List<String> ackIds = new ArrayList<>();
  private SparkSession sparkSession;
  private final HoodieDeltaStreamerMetrics metrics;

  public JsonPubSubSource(TypedProperties cfg, JavaSparkContext jssc,
                          SparkSession sparkSession, SchemaProvider schemaProvider,
                          HoodieDeltaStreamerMetrics metrics) {
    super(cfg, jssc , sparkSession, schemaProvider);
    this.metrics = metrics;
    this.subscriptionId = props.getString("hoodie.deltastreamer.source.pubsub.subscriptionId");
    String projectId = props.getString("hoodie.deltastreamer.source.pubsub.projectId");
    int numOfMessages = 1000;
    this.sparkSession = sparkSession;
    try {
      SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                      .build()).build();
      this.subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
      String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
      this.pullRequest =
          PullRequest.newBuilder()
              .setMaxMessages(numOfMessages)
              .setSubscription(subscriptionName)
              .build();
    } catch (IOException e) {
      LOG.error("Error building subscriber :: {}",e);
    }

  }

  @Override
  protected InputBatch<JavaRDD<String>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
    List<String> messages = new ArrayList<>();
    String checkPointStr = "no-messages";
    if (pullResponse.getReceivedMessagesList().isEmpty()) {
      LOG.warn("Receiving no messages from :: "+ this.subscriptionId);
    }else {
      List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();
      for (ReceivedMessage message : receivedMessages) {
        messages.add(message.getMessage().getData().toStringUtf8());
        ackIds.add(message.getAckId());
      }
      Collections.sort(ackIds);
      checkPointStr = ackIds.get(0)+"-"+ackIds.get(ackIds.size()-1);
    }
    JavaRDD<String> newDataRDD = sparkSession.createDataset(messages, Encoders.STRING()).toJavaRDD();
    return new InputBatch<>(Option.of(newDataRDD), checkPointStr);
  }

  @Override
  public void onCommit(String lastCkptStr) {
    try {
      if (ackIds.size() > 0) {
        if (lastCkptStr.equalsIgnoreCase(ackIds.get(0) + "-" + ackIds.get(ackIds.size() - 1))) {
          AcknowledgeRequest acknowledgeRequest =
              AcknowledgeRequest.newBuilder()
                  .setSubscription(subscriptionId)
                  .addAllAckIds(ackIds)
                  .build();

          // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
          subscriber.acknowledgeCallable().call(acknowledgeRequest);
        }
      }
    } catch (Exception e){
      LOG.error("Error while sending ack to pub sub queue :: {}",e.getCause());
      LOG.warn("The above error can result in the duplicate data");
    }finally {
      ackIds.clear();
    }
  }

}