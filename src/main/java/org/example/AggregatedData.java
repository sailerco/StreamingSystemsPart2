package org.example;

public class AggregatedData {
    private final int id;
    private final Double speed;
    private long firstTimestamp;
    private long lastTimestamp;

    public AggregatedData(int id, Double speed, long firstTimestamp, long lastTimestamp) {
        this.id = id;
        this.speed = speed;
        this.firstTimestamp = firstTimestamp;
        this.lastTimestamp = lastTimestamp;
    }

    public int getId() {
        return id;
    }

    public Double getSpeed() {
        return speed;
    }

    public long getFirstTimestamp() {
        return firstTimestamp;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }
}
