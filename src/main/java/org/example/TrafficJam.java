package org.example;

public class TrafficJam {
    double minSpeed;
    double maxSpeed;
    double speedReduction;
    int id;

    public TrafficJam(double minSpeed, double maxSpeed, double speedReduction, int id) {
        this.minSpeed = minSpeed;
        this.maxSpeed = maxSpeed;
        this.speedReduction = speedReduction;
        this.id = id;
    }
}
