package org.example;

import no.nav.common.KafkaEnvironment;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Properties;

import static java.util.Collections.emptyList;

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

    static int sensorCount = 5;

    static int timeframe = 1000;
    static Producer producer = new Producer();

    public static void main(String[] args) throws InterruptedException {
        env.start();
        Processor processor = new Processor();

        Thread t = new Thread(() -> {
            while (true) {
                try {
                    processor.consumeData();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.setName("Consume Data");
        t.start();
        Thread.sleep(5000);
        new TestGenerator().generateTestDataBatch();

        while (true) {
            Thread.sleep(1000);
            processor.calculateAverageSpeedForSensorsOverTime(sensorCount, timeframe);
        }
    }
}