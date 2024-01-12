package org.example;


import java.text.SimpleDateFormat;
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
    static int sensorCount = 3;
    private static Random random = new Random();

    public static String generateTestData() {
        timestamp = new Date(timestamp.getTime() + getRandomDelay());
        int sensorName = random.nextInt(sensorCount) + 1; //Random ID for Sensor between 1 and sensorCount
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

}
