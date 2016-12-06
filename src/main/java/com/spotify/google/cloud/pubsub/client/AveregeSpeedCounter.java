package com.spotify.google.cloud.pubsub.client;

/**
 * Created by badrock on 2016-11-30.
 */
public class AveregeSpeedCounter {
  private final static int SEC = 1000000000;
  private long speedIntervalNano;
  private long startTime;
  private long endTime;
  private double currSpeed;

  public AveregeSpeedCounter(long speedInterval, long fudgeMillis) {
    this.speedIntervalNano = speedInterval*1000000;
    long refTime = System.nanoTime();
    this.startTime = refTime - (refTime < 0 ? -1 * fudgeMillis : fudgeMillis)*1000000;
    this.endTime = this.startTime;
    this.currSpeed = 0.0;
  }

  public double getCurrSpeed() {
    return this.currSpeed;
  }

  public double getCurrNormalisedSpeed() {
    addPoint(0);
    return this.currSpeed;
  }

  public void addPoint(long amount) {
    long currTime = System.nanoTime();
    double winSpeed = (currSpeed * ((endTime - startTime)/(double)SEC));
    currSpeed = (winSpeed + amount)/((currTime - startTime)/(double)SEC + 0.0000001);

    endTime = currTime;
    if (startTime < currTime - speedIntervalNano) {
      startTime = currTime - speedIntervalNano;
    }
  }

  public double addPointAndGet(long amount) {
    addPoint(amount);
    return getCurrSpeed();
  }
}
