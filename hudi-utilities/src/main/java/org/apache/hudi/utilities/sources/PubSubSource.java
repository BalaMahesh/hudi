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

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.common.collect.Lists;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class PubSubSource extends JsonSource{

  protected SubscriberStub subscriber;
  protected PullRequest pullRequest;
  protected String subscriptionId = "";
  protected List<String> ackIds = new ArrayList<>();
  protected SparkSession sparkSession;
  protected final HoodieDeltaStreamerMetrics metrics;
  protected String fullSubscription;
  protected int numberOfPullRequests = 1;
  protected String projectId = "";
  protected String credentialPath = "";

  private static final Logger LOG = LogManager.getLogger(PubSubSource.class);

  public PubSubSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                      SchemaProvider schemaProvider,HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.metrics = metrics;
    try {
    this.subscriptionId = props.getString("hoodie.deltastreamer.source.pubsub.subscriptionId");
    this.projectId = props.getString("hoodie.deltastreamer.source.pubsub.projectId");
    this.credentialPath = props.getString("hoodie.deltastreamer.source.pubsub.auth.credentials");
    numberOfPullRequests = Integer.parseInt(props.containsKey("hoodie.deltastreamer.source.pubsub.max.pullRequests") ?
        props.getString("hoodie.deltastreamer.source.pubsub.max.pullRequests") : "1");

    int numOfMessages = 1000;
    this.sparkSession = sparkSession;
      SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                      .build()).setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(credentialPath)))).build();
      this.subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
      this.fullSubscription = ProjectSubscriptionName.format(projectId, subscriptionId);
      this.pullRequest =
          PullRequest.newBuilder()
              .setMaxMessages(numOfMessages)
              .setSubscription(fullSubscription)
              .build();
    } catch (Exception e) {
      LOG.error("Error Creating PullSource :: {}",e);
    }

  }


  protected List<ReceivedMessage> fetchNewMessages(Option<String> lastCkptStr, long sourceLimit) {
    List<ApiFuture<PullResponse>> futureList = new ArrayList<>(numberOfPullRequests);
    List<ReceivedMessage> receivedMessages = new ArrayList<>();
    int count = 0;
    while (count < numberOfPullRequests) {
      futureList.add(subscriber.pullCallable().futureCall(pullRequest));
      count++;
    }
    futureList.forEach(f->{
      try {
        PullResponse response = f.get();
        receivedMessages.addAll(response.getReceivedMessagesList());
        LOG.debug("Received :: "+response.getReceivedMessagesCount() +" messages from pull");
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error while fetching records :: {}",e);
      }
    });
    if (receivedMessages.size() == 0) {
      LOG.warn("Receiving no messages from :: " + this.subscriptionId);
      return Collections.emptyList();
    }else {
      LOG.info("Pulled total of :: "+receivedMessages.size() + " messages with :: "+numberOfPullRequests+" pull requests");
      return receivedMessages;
    }
  }

  @Override
  public void onCommit(String lastCkptStr) {
    try {
      if (ackIds.size() > 0) {
        List<ApiFuture<Empty>> futures = new ArrayList<>();
        List<List<String>> ackIdChunks = Lists.partition(ackIds, 2000);
        LOG.info("About to acknowledge:: " + ackIds.size() + " messages in :: " + ackIdChunks.size() + " requests");
        ackIdChunks.forEach(ackIdChunk -> {
          AcknowledgeRequest acknowledgeRequest =
              AcknowledgeRequest.newBuilder()
                  .setSubscription(fullSubscription)
                  .addAllAckIds(ackIdChunk)
                  .build();
          futures.add(subscriber.acknowledgeCallable().futureCall(acknowledgeRequest));
        });
        for (ApiFuture<Empty> future : futures) {
          try {
            if (!future.get(10, TimeUnit.SECONDS).isInitialized()) {
              LOG.error("Acknowledge request failed for one future, messages can get processed again");
            }
          } catch (Exception e) {
            LOG.error("Acknowledge request error", e);
          }
        }
        LOG.info("Acknowledged :: " + ackIds.size());
      } else {
        LOG.info("No ackIds to acknowledge the data");
      }
    }catch (Exception e){
      LOG.error("Error while acknowledging data:: {}",e);
    }finally {
      ackIds.clear();
    }
  }


}
