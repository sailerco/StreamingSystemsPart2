package org.example;

public class FlattenedData {

    // todo: gerne hier umbennenen, speed in avg speed
    private final int id;
    private final Double speed;
    private final long timestamp; // Neues Attribut für den Zeitstempel des Ereignisses

    public FlattenedData(int id, Double speed, long timestamp) {
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
