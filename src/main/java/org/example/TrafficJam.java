package org.example;

public class TrafficJam {

    double avgSpeed;

    double timestamp;


    public TrafficJam(double avgSpeed, double timestamp) {
        this.avgSpeed = avgSpeed;
        this.timestamp = timestamp;
    }

    public double getAvgSpeed() {
        return avgSpeed;
    }

    public double getTimestamp() {
        return timestamp;
    }
}
