package org.example;

import org.example.DataProcessing.Processor;
import org.example.Kafka.KafkaTopicCreator;
import org.example.Kafka.Producer;

import java.text.ParseException;
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
        Thread.sleep(4000);

        while (true) {
            Thread.sleep(1000);
            new TestGenerator().generate(producer, batchSize);
            Thread.sleep(100);
            processor.calculateAvgForEachSensorInTimeframe(sensorCount, timeframe);
            processor.displayAvg(timeframe);
            processor.displaySequence(new String[]{"1", "2", "3"}, 0, timeframe);
        }
    }
}