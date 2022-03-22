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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class PubSubSource extends JsonSource{

  protected SubscriberStub subscriber;
  protected PullRequest pullRequest;
  protected String subscriptionId = "";
  protected List<String> ackIds = new ArrayList<>();
  protected SparkSession sparkSession;
  protected final HoodieDeltaStreamerMetrics metrics;
  protected String fullSubscription;
  private int numberOfPullRequests = 1;

  private static final Logger LOG = LogManager.getLogger(PubSubSource.class);

  public PubSubSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                      SchemaProvider schemaProvider,HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.metrics = metrics;
    this.subscriptionId = props.getString("hoodie.deltastreamer.source.pubsub.subscriptionId");
    String projectId = props.getString("hoodie.deltastreamer.source.pubsub.projectId");
    this.fullSubscription = String.format("projects/%s/subscriptions/%s",projectId,subscriptionId);
    String credentialsPath = props.getString("hoodie.deltastreamer.source.pubsub.auth.credentials");
    numberOfPullRequests = Integer.parseInt(props.containsKey("hoodie.deltastreamer.source.pubsub.max.pullRequests") ?
        props.getString("hoodie.deltastreamer.source.pubsub.max.pullRequests") : "1");

    int numOfMessages = 1000;
    this.sparkSession = sparkSession;
    try {
      SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                      .build()).setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(credentialsPath)))).build();
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
        LOG.info("Received :: "+response.getReceivedMessagesCount() +" messages from pull");
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
        if (lastCkptStr.equalsIgnoreCase(ackIds.get(0) + "-" + ackIds.get(ackIds.size() - 1))) {
          AcknowledgeRequest acknowledgeRequest =
              AcknowledgeRequest.newBuilder()
                  .setSubscription(fullSubscription)
                  .addAllAckIds(ackIds)
                  .build();

          // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
          subscriber.acknowledgeCallable().call(acknowledgeRequest);
          LOG.info("Acknowledged " + ackIds.size() + " messages");
        }else{
          LOG.warn("Acknowledgement Ids not matching for the batch of :: "+ackIds.size() + " messages. No acknowledgement happening");
        }
      }
    } catch (Exception e) {
      LOG.error("Error while sending ack to pub sub queue :: {}", e.getCause());
      LOG.warn("The above error can result in the duplicate data");
    } finally {
      ackIds.clear();
    }
  }


}
