package org.example.DataProcessing;

import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class Data implements Serializable {
    public Date timestamp;
    public String id;
    public ArrayList<Double> speed;

    public Data(Date timestamp, String id, ArrayList<Double> speed) {
        this.timestamp = timestamp;
        this.id = id;
        this.speed = speed;
    }
}
