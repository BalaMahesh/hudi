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


import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials$;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import scala.Option$;
import scala.reflect.ClassTag$;

public class JsonPubSubSource extends JsonSource{

  private JavaSparkContext sparkContext;
  private JavaStreamingContext jssc;
  private String subscriptionId = "";
  private String projectId = "";
  List<String> ackIds = new ArrayList<>();
  private String fullSubscription;
  private SparkGCPCredentials sparkGCPCredentials;
  private SubscriberStub subscriber;
  private JavaReceiverInputDStream<SparkPubsubMessage> messages;



  private static final Logger LOG = LogManager.getLogger(JsonPubSubPullSource.class);


  public JsonPubSubSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                            SchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.sparkContext = sparkContext;
    this.subscriptionId = props.getString("hoodie.deltastreamer.source.pubsub.subscriptionId");
    projectId = props.getString("hoodie.deltastreamer.source.pubsub.projectId");
    String credentialFile =  props.getString("hoodie.deltastreamer.source.pubsub.auth.credentials");
    this.fullSubscription = String.format("projects/%s/subscriptions/%s",projectId,subscriptionId);
    this.sparkGCPCredentials = SparkGCPCredentials$.MODULE$.builder().jsonServiceAccount(credentialFile).build();
    try {
      SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                      .build()).build();
      this.subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
    }catch (IOException e){
      LOG.error("Error building Pub Sub subscriber :: {}",e);
    }
    this.jssc = new JavaStreamingContext(sparkContext, Durations.seconds(60));
    messages = PubsubUtils.createStream(jssc,projectId,subscriptionId,sparkGCPCredentials,StorageLevel.MEMORY_AND_DISK(),false);
    this.jssc.start();
  }

  @Override
  protected InputBatch<JavaRDD<String>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    String checkPointStr = "no-messages";
    JavaRDD<SparkPubsubMessage> messageRdd = sparkContext.emptyRDD();
    messages.foreachRDD(messageRdd::union);
    JavaRDD<String> inputMessages = messageRdd.map(message-> {
      return new String(message.getData(), StandardCharsets.UTF_8);
    });
    LOG.info("Received :: " + ackIds.size() + "messages from subscription :: " + this.subscriptionId);
    ackIds = messageRdd.map(SparkPubsubMessage::getAckId).collect();
    return new InputBatch<>(Option.of(inputMessages), checkPointStr);
  }

  @Override
  public void onCommit(String lastCkptStr) {
    try {
      if (ackIds.size() > 0) {
        if (lastCkptStr.equalsIgnoreCase(ackIds.get(0) + "-" + ackIds.get(ackIds.size() - 1))) {
          AcknowledgeRequest acknowledgeRequest =
              AcknowledgeRequest.newBuilder()
                  .setSubscription(fullSubscription)
                  .addAllAckIds(ackIds)
                  .build();

          // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
          subscriber.acknowledgeCallable().call(acknowledgeRequest);
          LOG.info("Acknowledged "+ ackIds.size() +" messages");
        }
      }else {
        LOG.info("No messages to acknowledge on subscription ::"+ this.subscriptionId);
      }
    } catch (Exception e){
      LOG.error("Error while sending ack to pub sub queue :: {}",e.getCause());
      LOG.warn("The above error can result in the duplicate data");
    }finally {
      ackIds.clear();
    }
  }

}
