package org.example;

import java.util.List;

public class SensorData {
    private int id;
    private List<Double> speed;
    private long timestamp; // Neues Attribut für den Zeitstempel des Ereignisses

    public SensorData(int id, List<Double> speed, long timestamp) {
        this.id = id;
        this.speed = speed;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public List<Double> getSpeed() {
        return speed;
    }

    public long getTimestamp() {
        return timestamp;
    }
}

