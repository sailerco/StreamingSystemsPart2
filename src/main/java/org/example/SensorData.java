package org.example;

import java.util.List;

public class SensorData {
    private int id;
    private List<Double> speed;
    private long eventTimestamp; // Neues Attribut für den Zeitstempel des Ereignisses

    public SensorData(int id, List<Double> speed, long eventTimestamp) {
        this.id = id;
        this.speed = speed;
        this.eventTimestamp = eventTimestamp;
    }

    public int getId() {
        return id;
    }

    public List<Double> getSpeed() {
        return speed;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }
}

