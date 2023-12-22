package org.example;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Processor {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    Consumer consumer = new Consumer();
    List<Data> results = new ArrayList<>();

    public void consumeData() throws ParseException {
        processData(consumer.getData(100));
    }


    private void processData(List<String> dataEntries) throws ParseException {
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


    public double calculateAvgInKMH(List<Double> speeds) {
        double sum = 0;
        for (double speed : speeds) {
            sum += speed * 3.6;
        }
        return sum / speeds.size();
    }

    //calculates avg speed for one sensor ->
    public void calculateAverageSpeedForSensors(int sensorCount, int timeframe) {
        for (int i = 1; i <= sensorCount; i++) {
            List<Double> speedsAtID = getSpeedsByID(i + "");
            if (speedsAtID.isEmpty()) System.out.println("Sensor " + i + " has not detected any speed");
            else {
                double avg = calculateAvgInKMH(getSpeedsByID(i + ""));
                System.out.println("Sensor " + i + " has an average speed of " + String.format(Locale.US, "%.2f", avg) + "km/h");
            }
        }
    }

    public List<Double> getSpeedsByID(String id) {
        List<Double> speedsAtID = new ArrayList<>();
        for (Data data : results) {
            if (data.id.equals(id)) speedsAtID.addAll(data.speed);
        }
        return speedsAtID;
    }


    public List<Double> getAllSpeeds() {
        List<Double> speedsAtID = new ArrayList<>();
        for (Data data : results) {
            speedsAtID.addAll(data.speed);
        }
        return speedsAtID;
    }

    //calculates the avg speed over all detected speeds
    public void calculateAverageSpeedOverAllSensors() {
        double avg = calculateAvgInKMH(getAllSpeeds());
        System.out.println("The average over the sensors was " + String.format(Locale.US, "%.2f", avg) + "km/h");
    }

    public void calculateAverageSpeedForSensorsOverTime(int sensorCount, int timeframe) {
        for (int i = 1; i <= sensorCount; i++) {
            Map<Long, ArrayList<Double>> speedsAtTime = getTimeAndSpeedOfID(i + "");
            if (speedsAtTime.isEmpty()) {
                System.out.println("Sensor " + i + " has not detected any speed");
            } else {
                ArrayList<String> avgs = calculate(speedsAtTime, timeframe, new ArrayList<>());
                System.out.println("Sensor " + i + " had in one or more timeframes of " + timeframe + "ms following mean speeds values: ");
                for (int j = 0; j <= avgs.size() - 1; j++) { //if-else is just for formatting purpose.
                    if (j < avgs.size() - 1)
                        System.out.print(avgs.get(j) + "km/h, ");
                    else
                        System.out.println(avgs.get(j) + "km/h.");
                }
            }
        }
    }

    public Map<Long, ArrayList<Double>> getTimeAndSpeedOfID(String id) {
        Map<Long, ArrayList<Double>> speedsAtTime = new LinkedHashMap<>();
        for (Data data : results) {
            if (data.id.equals(id)) {
                speedsAtTime.put(data.timestamp.getTime(), data.speed);
            }
        }
        return speedsAtTime;
    }

    //calculates for a given Map the avg Speed in a given timeframe. Works recursive
    public ArrayList<String> calculate(Map<Long, ArrayList<Double>> speedsAtTime, int timeFrame, ArrayList<String> totalMeanValues) {
        if (!speedsAtTime.isEmpty()) {
            Map<Long, ArrayList<Double>> copy = new LinkedHashMap<>();
            long startTime = speedsAtTime.entrySet().iterator().next().getKey();
            ArrayList<Double> speedsInFrame = new ArrayList<>();
            for (Map.Entry<Long, ArrayList<Double>> entry : speedsAtTime.entrySet()) {
                long currentTime = entry.getKey();
                if (currentTime - startTime < timeFrame) {
                    speedsInFrame.addAll(entry.getValue());
                } else {
                    copy.put(entry.getKey(), entry.getValue());
                }
            }
            if (!copy.isEmpty()) {
                totalMeanValues.add(String.format(Locale.US, "%.2f", calculateAvgInKMH(speedsInFrame)));
                calculate(copy, timeFrame, totalMeanValues); //Recursive
            }
            if (totalMeanValues.isEmpty())
                totalMeanValues.add(String.format(Locale.US, "%.2f", calculateAvgInKMH(speedsInFrame)));
        }
        return totalMeanValues;
    }
}
