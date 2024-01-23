package org.example;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class Listener {
    static class SensorEventListener implements UpdateListener {
        @Override
        public void update(EventBean[] newData, EventBean[] oldData, EPStatement statement, EPRuntime runtime) {
            //retrieve generated Data
            int id = (int) newData[0].get("id");
            List<Double> speed = (List<Double>) newData[0].get("speed");
            long timestamp = (long) newData[0].get("timestamp");

            //Filter the negative out
            speed.removeIf(value -> value <= 0.0);
            speed.removeIf(Objects::isNull);

            //convert to km/h
            speed.replaceAll(s -> s * 3.6);

            if (!speed.isEmpty()) {
                System.out.println(timestamp + " " + id + " " + speed);
                //flatten Data
                List<FlattenedData> flattenedData = new ArrayList<>();
                speed.forEach(s -> flattenedData.add(new FlattenedData(id, s, timestamp)));
                flattenedData.forEach(data -> runtime.getEventService().sendEventBean(data, "FlattenedData"));
            }
        }
    }

    static class AvgEventListener implements UpdateListener {
        @Override
        public void update(EventBean[] newData, EventBean[] oldData, EPStatement statement, EPRuntime runtime) {
            for (EventBean eventBean : newData) {
                int id = (int) eventBean.get("id");
                if (eventBean.get("averageSpeed") != null) {
                    double averageSpeed = (double) eventBean.get("averageSpeed");
                    System.out.println("Average speed for ID " + id + " is " + String.format(Locale.US, "%.2f km/h", averageSpeed));
                } else
                    System.out.println("There were no measurements for " + id + " in this window");
            }
        }
    }

    static class SeqAvgListener implements UpdateListener {
        @Override
        public void update(EventBean[] newData, EventBean[] oldData, EPStatement statement, EPRuntime runtime) {
            for (EventBean eventBean : newData) {
                if (newData != null && newData.length > 0) {
                    double averageSpeed = (double) newData[0].get("overallAverageSpeed");
                    System.out.println("Average speed over sequence is " + String.format(Locale.US, "%.2f km/h", averageSpeed));
                }
            }
        }
    }

    static class TrafficEventListener implements UpdateListener {
        @Override
        public void update(EventBean[] newData, EventBean[] oldData, EPStatement statement, EPRuntime runtime) {
            System.out.println("Traffic Jam detected");
            for (EventBean eventBean : newData) {
                int id = (int) eventBean.get("id");
                double min = (double) eventBean.get("minSpeed");
                double max = (double) eventBean.get("maxSpeed");
                long first = (long) eventBean.get("firstTimestamp");
                long last = (long) eventBean.get("lastTimestamp");
                System.out.println("There was a drop for " + id);
                TrafficJam trafficJam = new TrafficJam(id, max - min, first, last);
                runtime.getEventService().sendEventBean(trafficJam, "TrafficJam");
            }
        }
    }
}
