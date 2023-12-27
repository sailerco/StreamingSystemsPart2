package org.example;

import org.example.DataProcessing.Processor;
import org.example.Kafka.KafkaTopicCreator;
import org.example.Kafka.Producer;

import java.text.ParseException;
import java.util.UUID;

import static org.example.TestGenerator.sensorCount;

public class Main {
    static int batchSize = 10;
    static int timeframe = 30000;
    static Producer producer = new Producer();

    public static String topic = "Measurements" + UUID.randomUUID(); //random topic for testing purposes

    public static void main(String[] args) throws InterruptedException {
        new KafkaTopicCreator().createTopic(topic, 1);
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
        new TestGenerator().generateTestDataBatch(batchSize);

        Thread.sleep(1000);
        processor.calculateAvgForEachSensorInTimeframe(sensorCount, timeframe);
        processor.calculatesAvgSpeedInSection(new String[]{"1", "2", "3"}, 0, timeframe);
    }
}