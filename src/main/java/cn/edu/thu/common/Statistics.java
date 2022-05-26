package cn.edu.thu.common;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

public class Statistics {

  public AtomicLong fileNum = new AtomicLong(0);
  public AtomicLong recordNum = new AtomicLong(0);
  public AtomicLong pointNum = new AtomicLong(0);
  public AtomicLong timeCost = new AtomicLong(0); // unit: ns

  private final int vectorSize = 100;
  private static int pos = 0;
  public Vector<Long> writeLatency = new Vector<>(vectorSize);

  public void addLatency(long timestampInNanosecond) {
    if (writeLatency.size() < vectorSize) {
      writeLatency.add(timestampInNanosecond);
    } else {
      if (pos >= vectorSize) {
        pos = 0;
      }
      writeLatency.set(pos, timestampInNanosecond);
      pos++;
    }
  }

  public double getAverageLatencyInMillisecond() {
    long sum = 0;
    for (int i = 0; i <= writeLatency.size() - 1; i++) {
      sum += writeLatency.get(i);
    }
    return sum * 1.0 / writeLatency.size() / 1000_000F;
  }

  public Statistics() {

  }


  /**
   * @return points / s
   */
  public double speed() {
    return ((double) pointNum.get()) / ((double) timeCost.get()) * 1000_000_000L;
  }
}
