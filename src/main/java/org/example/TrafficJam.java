package org.example;

public class TrafficJam {
    private final int id;
    private final Double dropSpeed;
    private long firstTimestamp;
    private long lastTimestamp;

    public TrafficJam(int id, Double dropSpeed, long firstTimestamp, long lastTimestamp) {
        this.id = id;
        this.dropSpeed = dropSpeed;
        this.firstTimestamp = firstTimestamp;
        this.lastTimestamp = lastTimestamp;
    }

    public int getId() {
        return id;
    }

    public Double getSpeed() {
        return dropSpeed;
    }

    public long getFirstTimestamp() {
        return firstTimestamp;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }
}
