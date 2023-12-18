package org.example;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class TestGenerator {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    static DecimalFormatSymbols symbols = new DecimalFormatSymbols();
    private static Random random = new Random();

    //mesaurementCount = Anzahl der Geschwindigkeits-Variablen pro Sensor
    public static String generateTestData(int sensorCount, int maxMeasurementCount, int minSpeed, int maxSpeed) {
        symbols.setDecimalSeparator('.');
        DecimalFormat df = new DecimalFormat("#.##", symbols);
        Date timestamp = new Date();

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
}
