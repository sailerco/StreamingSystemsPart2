package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.DataProcessing.Data;
import org.example.DataProcessing.Processor;
import org.example.Kafka.KafkaTopicCreator;
import org.example.Kafka.Producer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public class Main {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    public static String topic = "Measurements" + UUID.randomUUID(); //random topic for testing purposes
    static int batchSize = 1;
    static int timeframe = 30000;
    static String bootstrapServers = "localhost:29092";

    public static void main(String[] args) throws InterruptedException {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        new KafkaTopicCreator().createTopic(topic, 1);

        Producer producer = new Producer();
        Processor processor = new Processor();

        Thread t = new Thread(() -> {
            while (true) {
                new TestGenerator().generate(producer, batchSize);
            }
        });

        t.start();
        Thread.sleep(1000);
        PCollection<String> collection = p.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServers)
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata()
        ).apply(Values.<String>create()).setCoder(StringUtf8Coder.of()); // PCollection<String>
        /*.apply("Print", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(@Element String input, OutputReceiver<Void> out) {
                        System.out.println(input);
                    }
                }));*/
        collection.apply("FilterEmptyValues", Filter.by(new SerializableFunction<String, Boolean>() { //TODO: is there a value with 0 speed
            public Boolean apply(String input) {
                return input.split(" ").length == 3;
            }
        }));
        collection.apply("Transform", ParDo.of(new DoFn<String, Data>() {
            @ProcessElement
            public void processElement(@Element String input, OutputReceiver<Data> output) throws ParseException {
                String[] splitValues = input.split(" ");
                if (splitValues.length > 2) {
                    ArrayList<Double> speeds = new ArrayList<>();
                    if (splitValues[2].contains(",")) {
                        String[] speedEntries = splitValues[2].split(",");
                        for (String speedEntry : speedEntries) {
                            double speed = Double.parseDouble(speedEntry);
                            if (speed >= 0) speeds.add(speed);
                        }
                        output.output(new Data(dateFormat.parse(splitValues[0]), splitValues[1], speeds));
                    } else if (Double.parseDouble(splitValues[2]) >= 0) {
                        speeds.add(Double.parseDouble(splitValues[2]));
                        output.output(new Data(dateFormat.parse(splitValues[0]), splitValues[1], speeds));
                    }
                }
            }
        })).apply("Print", ParDo.of(new DoFn<Data, Data>() {
                    @ProcessElement
                    public void processElement(@Element Data input, OutputReceiver<Data> out) {
                        System.out.print(input.timestamp + " " + input.id);
                        input.speed.forEach(s -> System.out.print(", " + s));
                        System.out.println();
                        out.output(input);
                    }
                })).setCoder(SerializableCoder.of(Data.class));

        //TODO: als nöchstes negative Werte raus machen
     /*   p.apply(ParDo.of(new DoFn<String, Data>() {
            @ProcessElement
            public Data filterMinus(@Element Data input, OutputReceiver<Void> out) {

            }
        }));*/


        p.run().waitUntilFinish();
    }

    public static void printAvg(Map<String, Map<Integer, Double>> avgs) {
        avgs.forEach((sensor, timeAndSpeeds) -> {
            if (timeAndSpeeds.isEmpty()) System.out.println("Sensor " + sensor + " has not detected any speed");
            else {
                System.out.print("Sensor " + sensor + " | timeframe(s) of length: " + timeframe + "ms | average speeds: ");
                timeAndSpeeds.forEach((window, speed) -> System.out.printf(Locale.US, "%.2f km/h (window number %d); ", speed, window));
                System.out.println();
            }
        });
    }
}