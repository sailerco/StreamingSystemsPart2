package org.example;

public class AggregatedData {
    private final int id;
    //todo: Auch hier speed umbennen in avgSpeed
    private final Double speed;
    private final long timestamp;

    public AggregatedData(int id, Double speed, long timestamp) {
        this.id = id;
        this.speed = speed;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public Double getSpeed() {
        return speed;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
