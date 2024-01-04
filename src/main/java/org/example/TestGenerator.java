package org.example;

import org.example.Kafka.Producer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

public class TestGenerator {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    static int m1 = 7000;
    static int m2 = 10000;
    static int minSpeed = 20; //m/s
    static int maxSpeed = 30; //m/s
    static int maxMeasurementCount = 4; //Maximum number of possible measured values
    static Date timestamp = new Date();
    public static int sensorCount = 3;
    private static Random random = new Random();
    static int sensorName;
    public static String generateTestData() {
        timestamp = new Date(timestamp.getTime() + getRandomDelay());
        sensorName = random.nextInt(sensorCount) + 1; //Random ID for Sensor between 1 and sensorCount
        int measurementCount = randomMeasurementCount();
        StringBuilder testData = new StringBuilder(dateFormat.format(timestamp) + " " + sensorName + " ");
        for (int i = 0; i < measurementCount; i++) {
            double s;
            if (random.nextDouble() < 0.1) //if random number is smaller than 10%, for testing purpose
                s = maxSpeed * -random.nextDouble(); // Generate a random negative value
            else
                s = minSpeed + (maxSpeed - minSpeed) * random.nextDouble(); // Generate value between max and min
            testData.append(String.format(Locale.US, "%.2f", s));
            if (i < measurementCount - 1)
                testData.append(",");
        }
        return testData.toString();
    }

    //random delay between 2 measured data
    public static int getRandomDelay() {
        return m1 + random.nextInt(m2 - m1 + 1);
    }

    //random number of measurements, there is a 10% probability for it to be 0
    public static int randomMeasurementCount() {
        if (random.nextDouble() < 0.1)
            return 0;
        else
            return random.nextInt(maxMeasurementCount) + 1;
    }

    public void generate(Producer producer, int batchSize){
        for (int i = 0; i < batchSize; i++) {
            producer.sendMessage(generateTestData(), sensorName);
        }
    }

    public void test(Producer producer){
        ArrayList<String> data = new ArrayList<>();
        data.add("2023-12-31T10:28:49.685Z 2 -15.85,21.21,28.48,21.60");
        data.add("2023-12-31T10:28:57.261Z 1 27.48,22.03");
        data.add("2023-12-31T10:29:06.918Z 1 -24.44");
        data.add("2023-12-31T10:29:16.191Z 2 26.48,26.64");
        data.add("2023-12-31T10:29:23.680Z 3 25.75");
        data.add("2023-12-31T10:29:31.244Z 1 25.71,24.24,29.77");
        data.add("2023-12-31T10:29:38.667Z 1 20.95,29.59,20.93,25.38");
        data.add("2023-12-31T10:29:48.062Z 3 21.53,26.26,28.28,21.46");
        data.add("2023-12-31T10:29:55.353Z 2 ");
        data.add("2023-12-31T10:30:03.473Z 1 26.23,23.51,-10.93");
        for(String d: data){
            String[] split = d.split(" ");
            System.out.println(split[1]);
            producer.sendMessage(d, Integer.parseInt(split[1]));
        }
    }
}
