package cn.edu.thu.common;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

public class Statistics {

    public AtomicLong fileNum = new AtomicLong(0);
    public AtomicLong recordNum = new AtomicLong(0);
    public AtomicLong pointNum = new AtomicLong(0);
    public AtomicLong timeCost = new AtomicLong(0); // unit: ns

    public Vector<Long> writeLatency = new Vector<>();


    public Statistics(){

    }

    /**
     * @return points / s
     */
    public double speed() {
        return ((double) pointNum.get()) / ((double) timeCost.get()) * 1000_000_000L;
    }
}
