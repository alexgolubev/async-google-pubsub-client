package com.spotify.google.cloud.pubsub.client;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AbstractPuller implements Closeable {
  protected final Acker acker;
  protected final Pubsub pubsub;
  protected final String project;
  protected final String subscription;
  protected final MessageHandler handler;
  protected final int maxConcurrency;
  protected final int batchSize;
  protected final int maxOutstandingMessages;
  protected final int maxAckQueueSize;
  protected final AtomicInteger outstandingRequests = new AtomicInteger();
  protected final AtomicInteger outstandingMessages = new AtomicInteger();
  protected final AtomicInteger messagesPulled = new AtomicInteger();

  protected final ScheduledExecutorService scheduler =
      MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));

  private static final Logger log = LoggerFactory.getLogger(AbstractPuller.class);

  protected AbstractPuller(Pubsub pubsub, String project,
                           String subscription, MessageHandler handler,
                           int maxConcurrency, int batchSize,
                           int maxOutstandingMessages, int maxAckQueueSize) {
    this.maxAckQueueSize = maxAckQueueSize;
    this.handler = Objects.requireNonNull(handler, "handler");
    this.maxOutstandingMessages = maxOutstandingMessages;
    this.project = Objects.requireNonNull(project, "project");
    this.batchSize = batchSize;
    this.subscription = Objects.requireNonNull(subscription, "subscription");
    this.maxConcurrency = maxConcurrency;
    this.pubsub = Objects.requireNonNull(pubsub, "pubsub");

    // Set up a batching acker for sending acks
    this.acker = Acker.builder()
                      .pubsub(pubsub)
                      .project(project)
                      .subscription(subscription)
                      .batchSize(batchSize)
                      .concurrency(maxConcurrency)
                      .queueSize(maxAckQueueSize)
                      .build();
  }

  public int maxAckQueueSize() {
    return maxAckQueueSize;
  }

  public int maxOutstandingMessages() {
    return maxOutstandingMessages;
  }

  public int outstandingMessages() {
    return outstandingMessages.get();
  }

  public int concurrency() {
    return maxConcurrency;
  }

  public int outstandingRequests() {
    return outstandingRequests.get();
  }

  public int batchSize() {
    return batchSize;
  }

  public String subscription() {
    return subscription;
  }

  public String project() {
    return project;
  }

  protected abstract void pull();

  protected void pullBatch(final boolean returnImmediately) {
    outstandingRequests.incrementAndGet();

    pubsub.pull(project, subscription, returnImmediately, batchSize)
        .whenComplete((messages, ex) -> {

          outstandingRequests.decrementAndGet();
          // Bail if pull failed
          if (ex != null) {
            AbstractPuller.log.error("Pull failed", ex);
            return;
          }

          // Add entire batch to outstanding message count
          outstandingMessages.addAndGet(messages.size());
          messagesPulled.addAndGet(messages.size());
          updatePullingSpeed(messages.size());

          // Call handler for each received message
          for (final ReceivedMessage message : messages) {
            final CompletionStage<String> handlerFuture;
            try {
              handlerFuture = handler.handleMessage(this, subscription, message.message(), message.ackId());
            } catch (Exception e) {
              outstandingMessages.decrementAndGet();
              AbstractPuller.log.error("Message handler threw exception", e);
              continue;
            }

            if (handlerFuture == null) {
              outstandingMessages.decrementAndGet();
              AbstractPuller.log.error("Message handler returned null");
              continue;
            }

            // Decrement the number of outstanding messages when handling is complete
            handlerFuture.whenComplete((ignore, throwable) -> outstandingMessages.decrementAndGet());

            // Ack when the message handling successfully completes
            handlerFuture.thenAccept(ack -> {
              acker.acknowledge(ack);
              updateAckingSpeed(1);
            }).exceptionally(throwable -> {
              if (!(throwable instanceof CancellationException)) {
                AbstractPuller.log.error("Acking pubsub threw exception", throwable);
              }
              return null;
            });
          }
        });
  }

  // The following functions are supposed to be overwritten by the subclasses
  // that need to keep track of the avg pulling and acking speeds
  protected void updatePullingSpeed(int amount) {}

  protected void updateAckingSpeed(int amount) {}

  @Override
  public void close() throws IOException {
    scheduler.shutdownNow();
    try {
      scheduler.awaitTermination(30, SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * A handler for received messages.
   */
  public interface MessageHandler {

    /**
     * Called when a {@link Puller} receives a message.
     *
     * @param puller       The {@link Puller}
     * @param subscription The subscription that the message was received on.
     * @param message      The message.
     * @param ackId        The ack id.
     * @return A future that should be completed with the ack id when the message has been consumed.
     */
    CompletionStage<String> handleMessage(AbstractPuller puller, String subscription, Message message, String ackId);
  }
}
