package org.example;

import org.example.DataProcessing.Processor;
import org.example.Kafka.KafkaTopicCreator;
import org.example.Kafka.Producer;

import java.text.ParseException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.example.TestGenerator.sensorCount;

public class Main {
    public static String topic = "Measurements" + UUID.randomUUID(); //random topic for testing purposes
    static int batchSize = 10;
    static int timeframe = 30000;

    public static void main(String[] args) throws InterruptedException {
        new KafkaTopicCreator().createTopic(topic, 1);
        Producer producer = new Producer();
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
        t.start();
        Thread.sleep(5000);

        while (true) {
            new TestGenerator().generateTestDataBatch(producer, batchSize);
            Thread.sleep(1000);
            printAvg(processor.calculateAvgForEachSensorInTimeframe(sensorCount, timeframe));
            processor.calculatesAvgSpeedInSection(new String[]{"1", "2", "3"}, 0, timeframe);
        }
    }

    public static void printAvg(Map<String, Map<Integer, Double>> avgs) {
        avgs.forEach((sensor, timeAndSpeeds) -> {
            if (timeAndSpeeds.isEmpty())
                System.out.println("Sensor " + sensor + " has not detected any speed");
            else {
                System.out.print("Sensor " + sensor + " | timeframe(s) of length: " + timeframe + "ms | average speeds: ");
                timeAndSpeeds.forEach((window, speed) -> {
                    System.out.print(String.format(Locale.US, "%.2f km/h (window number %d); ", speed, window));
                });
                System.out.println();
            }
        });
    }
}