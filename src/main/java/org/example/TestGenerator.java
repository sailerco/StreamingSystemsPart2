package org.example;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import static org.example.Main.producer;
import static org.example.Main.sensorCount;

public class TestGenerator {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    static int m1 = 1000;
    static int m2 = 2000;
    static int minSpeed = 10;
    static int maxSpeed = 30;
    static int maxMeasurementCount = 4;
    static DecimalFormatSymbols symbols = new DecimalFormatSymbols();
    private static Random random = new Random();
    static Date timestamp;

    //mesaurementCount = Anzahl der Geschwindigkeits-Variablen pro Sensor
    public static String generateTestData(int sensorCount, int maxMeasurementCount, int minSpeed, int maxSpeed) {
        symbols.setDecimalSeparator('.');
        DecimalFormat df = new DecimalFormat("#.##", symbols);
        timestamp = new Date(timestamp.getTime() + getRandomDelay());
        int sensorName = random.nextInt(sensorCount) + 1; //Random ID for Sensor between 1 and sensorCount
        int measurementCount = random.nextInt(maxMeasurementCount);
        StringBuilder testData = new StringBuilder(dateFormat.format(timestamp) + " " + sensorName + " ");
        for (int i = 0; i < measurementCount; i++) {
            double s;
            if (random.nextDouble() < 0.1) //if random number is smaller than 10%, for testing purpose
                s = maxSpeed * -random.nextDouble(); // Generate a random negative value
            else
                s = minSpeed + (maxSpeed - minSpeed) * random.nextDouble(); // Generate Value between max and min

            testData.append(df.format(s));
            if (i < measurementCount - 1)
                testData.append(",");
        }
        return testData.toString();
    }

    public void generateTestDataBatch() throws InterruptedException {
        ArrayList<String> data = new ArrayList<>();
        timestamp = new Date();
        for (int i = 0; i < sensorCount; i++) {
            data.add(generateTestData(sensorCount, maxMeasurementCount, minSpeed, maxSpeed));
        }
        producer.sendMessageList(data); //sends data in a batch (didn't work in single calls >:()
    }

    private static int getRandomDelay(){
        return m1 + random.nextInt(m2 - m1 + 1);
    }
}
