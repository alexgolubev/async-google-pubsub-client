package com.spotify.google.cloud.pubsub.client;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Created by badrock on 2016-12-01.
 */
public class AverregeSpeedCounterTest {

  public static final int SPEED_INTERVAL_MS = 100;
  public static final int FUDGE = 10;

  @Test
  public void addConstantValues() throws InterruptedException {
    AveregeSpeedCounter counter = new AveregeSpeedCounter(SPEED_INTERVAL_MS, FUDGE);
    for (int i = 0; i < 50; i++) {
      counter.addPoint(1);
      Thread.sleep(5);
    }
    assertThat(counter.getCurrSpeed(), allOf(greaterThan(190.0), lessThan(200.0)));
  }

  @Test
  public void increaseSpeed() throws InterruptedException {
    AveregeSpeedCounter counter = new AveregeSpeedCounter(SPEED_INTERVAL_MS, FUDGE);
    for (int i = 0; i < 50; i++) {
      counter.addPoint(1);
      Thread.sleep(5);

    }
    assertThat(counter.getCurrSpeed(), allOf(greaterThan(190.0), lessThan(200.0)));

    for (int i = 0; i < 50; i++) {
      counter.addPoint(2);
      Thread.sleep(5);
    }
    assertThat(counter.getCurrSpeed(), allOf(greaterThan(350.0), lessThan(400.0)));
  }

  @Test
  public void decreaseSpeed() throws InterruptedException {
    AveregeSpeedCounter counter = new AveregeSpeedCounter(SPEED_INTERVAL_MS, FUDGE);
    for (int i = 0; i < 50; i++) {
      counter.addPoint(2);
      Thread.sleep(5);
    }
    assertThat(counter.getCurrSpeed(), allOf(greaterThan(380.0), lessThan(400.0)));

    for (int i = 0; i < 50; i++) {
      counter.addPoint(1);
      Thread.sleep(5);
    }
    assertThat(counter.getCurrSpeed(), allOf(greaterThan(200.0), lessThan(220.0)));
  }

  @Test
  public void getNormalisedSpeed() throws InterruptedException {
    AveregeSpeedCounter counter = new AveregeSpeedCounter(SPEED_INTERVAL_MS, FUDGE);
    for (int i = 0; i < 50; i++) {
      counter.addPoint(1);
      Thread.sleep(5);
    }
    Thread.sleep(SPEED_INTERVAL_MS/2);
    assertThat(counter.getCurrSpeed(), allOf(greaterThan(190.0), lessThan(200.0)));
    assertThat(counter.getCurrNormalisedSpeed(), allOf(greaterThan(100.0), lessThan(120.0)));
  }

  @Test
  public void zeroSpeed() throws InterruptedException {
    AveregeSpeedCounter counter = new AveregeSpeedCounter(SPEED_INTERVAL_MS, FUDGE);
    assertThat(counter.getCurrSpeed(), is(0.0));
  }

  @Test
  public void zeroIncrementsSpeed() throws InterruptedException {
    AveregeSpeedCounter counter = new AveregeSpeedCounter(SPEED_INTERVAL_MS, FUDGE);
    for (int i = 0; i < 50; i++) {
      counter.addPoint(0);
      Thread.sleep(5);
    }
    assertThat(counter.getCurrSpeed(), is(0.0));
  }
}
