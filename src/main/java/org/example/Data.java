package org.example;

import java.util.ArrayList;
import java.util.Date;

public class Data {
    Date timestamp;
    String id;
    ArrayList<Double> speed;

    public Data(Date timestamp, String id, ArrayList<Double> speed) {
        this.timestamp = timestamp;
        this.id = id;
        this.speed = speed;
    }
}
