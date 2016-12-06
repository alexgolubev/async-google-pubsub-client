package com.spotify.google.cloud.pubsub.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created by badrock on 2016-11-24.
 */
public class DynamicPuller extends AbstractPuller {

  private static final Logger log = LoggerFactory.getLogger(DynamicPuller.class);

  public static final double OPTIMISTIC_PULLING = 0.9; // pull data 10% faster then it is acked
  private final long minPullIntervalMillis;
  private final long maxPullIntervalMillis;
  private final long initialPullIntervalMillis;
  private final long maxQueueSizeMegaBytes;
  private final long consumerFlushInterval;
  private long currentPullIntervalMillis;
  private long outstandingMessagesPrePull;
  private AveregeSpeedCounter pullSpeedCounter;
  private AveregeSpeedCounter ackSpeedCounter;
  private AveregeSpeedCounter pullRateCounter;

  protected DynamicPuller(final Builder builder) {
    super(builder.pubsub, builder.project,
          builder.subscription, builder.handler, builder.concurrency,
          builder.batchSize, builder.maxOutstandingMessages, builder.maxAckQueueSize);
    this.minPullIntervalMillis = builder.minPullIntervalMillis;
    this.maxPullIntervalMillis = builder.maxPullIntervalMillis;
    this.initialPullIntervalMillis = builder.initialPullIntervalMillis;
    this.currentPullIntervalMillis = this.initialPullIntervalMillis;
    this.maxQueueSizeMegaBytes = builder.maxQueueSizeMegaBytes;
    this.consumerFlushInterval = builder.consumerFlushInterval;
    this.pullSpeedCounter = new AveregeSpeedCounter(maxPullIntervalMillis, initialPullIntervalMillis);
    this.ackSpeedCounter = new AveregeSpeedCounter(consumerFlushInterval, consumerFlushInterval/5);
    this.pullRateCounter = new AveregeSpeedCounter(maxPullIntervalMillis, initialPullIntervalMillis);

    log.info("Starting PubSub Dynamic Puller");

    pull();

    // Schedule pulling to compensate for failures and exceeding the outstanding message limit
    scheduler.scheduleWithFixedDelay(this::checkForFirstPull, initialPullIntervalMillis,
                                     initialPullIntervalMillis, MILLISECONDS);
  }

  @Override
  protected void pull() {
    if (this.outstandingRequests() < maxConcurrency &&
        outstandingMessages.get()-batchSize < maxOutstandingMessages) {
      pullRateCounter.addPoint(1);
      outstandingMessagesPrePull = outstandingMessages();
      pullBatch(false);
      scheduleNextPull();
    }
  }

  private void scheduleNextPull() {
    final long msgsReceived = messagesPulled.getAndSet(0);
    final long msgsAckedSinceLastPull = outstandingMessagesPrePull + msgsReceived - outstandingMessages();
    double pullSpeed = pullSpeedCounter.addPointAndGet(msgsReceived);
    double ackSpeed = ackSpeedCounter.addPointAndGet(msgsAckedSinceLastPull);
    double pullToAckRatio = pullSpeed / ackSpeed;

    adjustPullInterval(pullToAckRatio);
    scheduler.schedule(this::pull, currentPullIntervalMillis, MILLISECONDS);
  }

  private void adjustPullInterval(double pullToAckRatio) {
    // keep pulling while we don't receive acks
    // TODO timeout on 5sec?
    if (ackSpeedCounter.getCurrSpeed() == 0.0) {
      return;
    }

    // We are pulling faster than we are acking => slow down
    if (pullToAckRatio>1) {
      currentPullIntervalMillis *= pullToAckRatio;
      return;
    }

    // check that we have data to pull.
    double maxTheoreticalPullRate = pullRateCounter.getCurrNormalisedSpeed() * batchSize();
    if (pullSpeedCounter.getCurrSpeed() < maxTheoreticalPullRate / 2) {
      currentPullIntervalMillis = Math.min(currentPullIntervalMillis/2, minPullIntervalMillis);
      return;
    }

    // Acking faster than pulling => speed up
    currentPullIntervalMillis *= pullToAckRatio*OPTIMISTIC_PULLING;
  }

  private void checkForFirstPull() {
    if (outstandingMessages()==0 || pullSpeedCounter.getCurrNormalisedSpeed()==0.0) {
      pull();
    }
  }

  public long minPullIntervalMillis() {
    return minPullIntervalMillis;
  }

  public long maxPullIntervalMillis() {
    return maxPullIntervalMillis;
  }

  public long initialPullIntervalMillis() {
    return initialPullIntervalMillis;
  }

  public long maxQueueSizeMegaBytes() {
    return maxQueueSizeMegaBytes;
  }

  public long consumerFlushInterval() {
    return consumerFlushInterval;
  }

  /**
   * Create a builder that can be used to build a {@link DynamicPuller}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder that can be used to build a {@link DynamicPuller}.
   */
  public static class Builder {

    private Pubsub pubsub;
    private String project;
    private String subscription;
    private MessageHandler handler;
    private int concurrency = 64;
    private int batchSize = 10000;
    private int maxOutstandingMessages = 640_000;
    private int maxAckQueueSize = 10 * batchSize;
    private long minPullIntervalMillis = 10;
    private long maxPullIntervalMillis = 5000;
    private long initialPullIntervalMillis = 500;
    private long maxQueueSizeMegaBytes = 1024;
    private long consumerFlushInterval = 5000;

    /**
     * Set the {@link Pubsub} client to use. The client will be closed when this {@link DynamicPuller} is closed.
     *
     * <p>Note: The client should be configured to at least allow as many connections as the maxConcurrency level of this
     * {@link DynamicPuller}.</p>
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
     * Set the Google Cloud Pub/Sub pull batch size. Default is {@code 10000}.
     */
    public Builder batchSize(final int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Set the limit of outstanding messages pending handling. Pulling is throttled when this limit is hit. Default is
     * {@code 640000}.
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
     * Set the minimum pull interval in millis. Default is {@code 10} millis.
     */
    public Builder minPullIntervalMillis(final long minPullIntervalMillis) {
      this.minPullIntervalMillis = minPullIntervalMillis;
      return this;
    }

    /**
     * Set the maximum pull interval in millis. Default is {@code 5000} millis.
     */
    public Builder maxPullIntervalMillis(final long maxPullIntervalMillis) {
      this.maxPullIntervalMillis = maxPullIntervalMillis;
      return this;
    }

    /**
     * Set the initial pull interval in millis. Default is {@code 500} millis.
     */
    public Builder initialPullIntervalMillis(final long initialPullIntervalMillis) {
      this.initialPullIntervalMillis = initialPullIntervalMillis;
      return this;
    }

    /**
     * Set the limit for the queues, acks and outstanding, in Bytes. Pulling is throttled when this limit is hit. Default is
     * {@code 1024}.
     */
    public Builder maxQueueSizeMegaBytes(final int maxQueueSizeMegaBytes) {
      this.maxQueueSizeMegaBytes = maxQueueSizeMegaBytes;
      return this;
    }

    /**
     * Set maximum flush interval for the consumer. The consumer flushes events in batches and send acks only once it
     * succeds, Delaulf is {@code 5000} millis
     */
    public Builder consumerFlushInterval(final long consumerFlushInterval) {
      this.consumerFlushInterval = consumerFlushInterval;
      return this;
    }

    /**
     * Build a {@link DynamicPuller}.
     */
    public DynamicPuller build() {
      return new DynamicPuller(this);
    }
  }

}
