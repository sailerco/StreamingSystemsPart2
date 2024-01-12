package org.example;

public class SensorData {
    private int id;
    private double speed;

    public SensorData(int id, double speed) {
        this.id = id;
        this.speed = speed;
    }

    public int getId() {
        return id;
    }

    public double getSpeed() {
        return speed;
    }

}
