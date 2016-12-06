/*
 * Copyright (c) 2011-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.google.cloud.pubsub.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Puller extends AbstractPuller {

  private static final Logger log = LoggerFactory.getLogger(Puller.class);

  private final long pullIntervalMillis;

  public Puller(final Builder builder) {
    super(builder.pubsub, builder.project,
          builder.subscription, builder.handler, builder.concurrency,
          builder.batchSize, builder.maxOutstandingMessages, builder.maxAckQueueSize);
    this.pullIntervalMillis = builder.pullIntervalMillis;

    log.info("Starting PubSub Puller");

    // Start pulling
    pull();

    // Schedule pulling to compensate for failures and exceeding the outstanding message limit
    scheduler.scheduleWithFixedDelay(this::pull, pullIntervalMillis, pullIntervalMillis, MILLISECONDS);
  }

  public long pullIntervalMillis() {
    return pullIntervalMillis;
  }

  @Override
  protected void pull() {
    while (this.outstandingRequests() < maxConcurrency &&
           outstandingMessages.get()-batchSize < maxOutstandingMessages) {
      pullBatch(true);
    }
  }

  /**
   * Create a builder that can be used to build a {@link Puller}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder that can be used to build a {@link Puller}.
   */
  public static class Builder {

    private Pubsub pubsub;
    private String project;
    private String subscription;
    private MessageHandler handler;
    private int concurrency = 64;
    private int batchSize = 1000;
    private int maxOutstandingMessages = 64_000;
    private int maxAckQueueSize = 10 * batchSize;
    private long pullIntervalMillis = 1000;

    /**
     * Set the {@link Pubsub} client to use. The client will be closed when this {@link Puller} is closed.
     *
     * <p>Note: The client should be configured to at least allow as many connections as the maxConcurrency level of this
     * {@link Puller}.</p>
     */
    public Builder pubsub(final Pubsub pubsub) {
      this.pubsub = pubsub;
      return this;
    }

    /**
     * Set the Google Cloud project to pull from.
     */
    public Builder project(final String project) {
      this.project = project;
      return this;
    }

    /**
     * The subscription to pull from.
     */
    public Builder subscription(final String subscription) {
      this.subscription = subscription;
      return this;
    }

    /**
     * The handler to call for received messages.
     */
    public Builder messageHandler(final MessageHandler messageHandler) {
      this.handler = messageHandler;
      return this;
    }

    /**
     * Set the Google Cloud Pub/Sub request maxConcurrency level. Default is {@code 64}.
     */
    public Builder concurrency(final int concurrency) {
      this.concurrency = concurrency;
      return this;
    }

    /**
     * Set the Google Cloud Pub/Sub pull batch size. Default is {@code 1000}.
     */
    public Builder batchSize(final int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Set the limit of outstanding messages pending handling. Pulling is throttled when this limit is hit. Default is
     * {@code 64000}.
     */
    public Builder maxOutstandingMessages(final int maxOutstandingMessages) {
      this.maxOutstandingMessages = maxOutstandingMessages;
      return this;
    }

    /**
     * Set the max size for the queue of acks back to Google Cloud Pub/Sub. Default is {@code 10 * batchSize}.
     */
    public Builder maxAckQueueSize(final int maxAckQueueSize) {
      this.maxAckQueueSize = maxAckQueueSize;
      return this;
    }

    /**
     * Set the pull interval in millis. Default is {@code 1000} millis.
     */
    public Builder pullIntervalMillis(final long pullIntervalMillis) {
      this.pullIntervalMillis = pullIntervalMillis;
      return this;
    }

    /**
     * Build a {@link Puller}.
     */
    public Puller build() {
      return new Puller(this);
    }
  }
}
