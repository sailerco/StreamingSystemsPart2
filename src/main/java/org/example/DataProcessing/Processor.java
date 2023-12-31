package org.example.DataProcessing;

import org.example.Kafka.Consumer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Processor {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    Consumer consumer = new Consumer();
    List<Data> results = new ArrayList<>();
    Map<String, Map<Integer, Double>> processedData = new HashMap<>(); //ID : (Time, Speed)

    public void consumeData() throws ParseException {
        processData(consumer.getData(1000));
    }

    //prepares Data from consumer, for example negative values or empty values are ignored.
    public void processData(List<String> dataEntries) throws ParseException {
        for (String entry : dataEntries) {
            String[] splitValues = entry.split(" ");
            if (splitValues.length > 2) {
                ArrayList<Double> speeds = new ArrayList<>();
                if (splitValues[2].contains(",")) {
                    String[] speedEntries = splitValues[2].split(",");
                    for (String speedEntry : speedEntries) {
                        double speed = Double.parseDouble(speedEntry);
                        if (speed >= 0) speeds.add(speed);
                    }
                    results.add(new Data(dateFormat.parse(splitValues[0]), splitValues[1], speeds));
                } else if (Double.parseDouble(splitValues[2]) >= 0) {
                    speeds.add(Double.parseDouble(splitValues[2]));
                    results.add(new Data(dateFormat.parse(splitValues[0]), splitValues[1], speeds));
                }
            }
        }
    }


    //iterates over the sensors and retrieves the avg speeds in a given timeframe (by calling other functions) (Aufgabe 1)
    public Map<String, Map<Integer, Double>> calculateAvgForEachSensorInTimeframe(int sensorCount, int timeframe) {
        for (int i = 1; i <= sensorCount; i++) {
            Map<Long, ArrayList<Double>> speedsAtTime = getTimeAndSpeedOfID(i + "");
            if (!speedsAtTime.isEmpty()) {
                processedData.put(i + "", calculateInTimeframe(speedsAtTime, new HashMap<>(), timeframe));
            }
        }
        return processedData;
    }

    //computes the avg speed over the given sensor seq and at a specific timeframe number (Aufgabe 2)
    public void calculatesAvgSpeedInSection(String[] sensorSeq, int timeframeNumber, int timeframe) {
        ArrayList<Double> avgSpeeds = new ArrayList<>();
        for (String id : sensorSeq) {
            if (processedData.get(id) != null && processedData.get(id).get(timeframeNumber) != null)
                avgSpeeds.add(processedData.get(id).get(timeframeNumber));
        }
        double res = calculateAvgInKMH(avgSpeeds, false);
        System.out.println("In the " + timeframeNumber + "# time-window there was a avg speed of "
                + String.format(Locale.US, "%.2f", res) + "km/h over the sensor sequence " + Arrays.toString(sensorSeq));
        Pair<Date> window = getTimeframeRange(timeframeNumber, timeframe);
        System.out.println("--> Time window was between " + window.first() + " and " + window.second());
    }


    //calculates for a given Map the avg speed in a given timeframe and also retrieves timeframe number. Works recursive
    private Map<Integer, Double> calculateInTimeframe(Map<Long, ArrayList<Double>> speedsAtTime, Map<Integer, Double> timeFrameSpeedsMap, int timeFrame) {
        Map<Long, ArrayList<Double>> copy = new LinkedHashMap<>();
        ArrayList<Double> speedsInFrame = new ArrayList<>();
        long startTime = speedsAtTime.entrySet().iterator().next().getKey();

        for (Map.Entry<Long, ArrayList<Double>> entry : speedsAtTime.entrySet()) {
            long currentTime = entry.getKey();
            if (currentTime - startTime < timeFrame) speedsInFrame.addAll(entry.getValue());
            else copy.put(entry.getKey(), entry.getValue());
        }

        int frameCount = getTimeframeNumber(startTime, timeFrame);
        timeFrameSpeedsMap.put(frameCount, calculateAvgInKMH(speedsInFrame, true));

        if (!copy.isEmpty()) calculateInTimeframe(copy, timeFrameSpeedsMap, timeFrame); //Recursive

        return timeFrameSpeedsMap;
    }

    //retrieves a map of measured speeds at a specific time for an id
    private Map<Long, ArrayList<Double>> getTimeAndSpeedOfID(String id) {
        Map<Long, ArrayList<Double>> speedsAtTime = new LinkedHashMap<>();
        for (Data data : results) {
            if (data.id.equals(id)) {
                speedsAtTime.put(data.timestamp.getTime(), data.speed);
            }
        }
        return speedsAtTime;
    }

    //calculation of avg speed in km/h (can be used for values in km/h or m/s)
    private double calculateAvgInKMH(List<Double> speeds, Boolean meterPerSecond) {
        double sum = 0;
        for (double speed : speeds) {
            if (meterPerSecond) sum += speed * 3.6;
            else sum += speed;
        }
        return sum / speeds.size();
    }

    //returns the timeframe number based on the first occurring ate and the timeframe
    private int getTimeframeNumber(long time, int timeframe) {
        long first = results.getFirst().timestamp.getTime();
        if (time >= first) return (int) ((time - first) / timeframe);
        else return -1;
    }

    //returns the start and end date based on the given timeframe number
    private Pair<Date> getTimeframeRange(int timeframeNumber, int timeframe) {
        long first = results.getFirst().timestamp.getTime();

        long start = first + ((long) timeframe * timeframeNumber);
        long end = first + ((long) timeframe * (timeframeNumber + 1));

        return new Pair<>(new Date(start), new Date(end));
    }
}
