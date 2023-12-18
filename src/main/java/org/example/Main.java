package org.example;

import no.nav.common.KafkaEnvironment;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import static java.util.Collections.emptyList;
import static org.example.TestGenerator.generateTestData;

public class Main {
    public static KafkaEnvironment env = new KafkaEnvironment(
            1,
            Arrays.asList("Measurements"),
            emptyList(),
            false,
            false,
            emptyList(),
            false,
            new Properties()
    );
    static int m1 = 100;
    static int m2 = 500;
    static int sensorCount = 5;
    static int minSpeed = 10;
    static int maxSpeed = 30;
    static int maxMeasurementCount = 4;
    private static Random random = new Random();
    static int delay = m1 + random.nextInt(m2 - m1 + 1);

    public static void main(String[] args) throws InterruptedException {
        env.start();

        Thread.sleep(4000);

        Consumer consumer = new Consumer();

        Thread t = new Thread(() -> {
            while (true) {
                consumer.getData(4000);
            }
        });
        t.start();

        Producer producer = new Producer();
        for (int i = 0; i < sensorCount; i++) {
            String testData = generateTestData(sensorCount, maxMeasurementCount, minSpeed, maxSpeed);
            producer.sendMessage(testData);
            System.out.println(testData);
            Thread.sleep(delay);
        }
    }
}