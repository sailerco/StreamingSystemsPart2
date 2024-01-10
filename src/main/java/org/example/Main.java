/*package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.DataProcessing.Data;
import org.example.DataProcessing.Processor;
import org.example.Kafka.KafkaTopicCreator;
import org.example.Kafka.Producer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
        ).apply(Values.<String>create()).setCoder(StringUtf8Coder.of());
        collection.apply("FilterEmptyValues", Filter.by(new SerializableFunction<String, Boolean>() { //TODO: is there a value with 0 speed
                    public Boolean apply(String input) {
                        return input.split(" ").length == 3;
                    }
                }))
                .apply("Transform", ParDo.of(new DoFn<String, Data>() {
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
                }))
                .apply("FilterMinus", Filter.by((SerializableFunction<Data, Boolean>) input -> input.speed.stream().allMatch(v -> v >= 0)))
                /*.apply("WithTimeStamp", ParDo.of(new DoFn<Data, Data>() {
                    @ProcessElement
                    public void processElement(@Element Data input, OutputReceiver<Data> out) {
                        Instant timestamp = new Instant(input.timestamp.getTime() + 1000);
                        out.outputWithTimestamp(input, timestamp);
                    }
                })) //TODO: the Code just works when we do not have this function or not the groupby, find a solution
                .apply("TimeWindow", Window.<Data>into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply(WithKeys.of(data -> {
                    assert data != null;
                    return data.id;
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Data.class)))
                .apply(GroupByKey.<String, Data>create())

                .apply("Print", ParDo.of(new DoFn<KV<String, Iterable<Data>>, Void>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Iterable<Data>> input, OutputReceiver<Void> out) {
                        System.out.println(input.getKey() + " " + input.getValue());
                    }
                }));
                .apply("avgForEachSensor", ParDo.of(new DoFn<KV<String, Iterable<Data>>, Double>() {
                    @ProcessElement
                    public void avg(@Element KV<String, Iterable<Data>> input, OutputReceiver<Double> out) {
                        double sum = 0;
                        double count = 0;
                        for (Data d : input.getValue()) {
                            for (double s : d.speed) {
                                count++;
                                sum += s * 3.6;
                            }
                        }
                        System.out.println(input.getKey() + " " + sum / count);
                        out.output(sum / count);
                    }
                }));*/

                /*.apply("Print", ParDo.of(new DoFn<KV<String, Iterable<Data>>, Void>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Iterable<Data>> input, OutputReceiver<Void> out) {
                        System.out.println(input.getKey() + " " + input.getValue());
                        for(Data d : input.getValue()){
                            System.out.print(d.id + " " + d.timestamp);
                            d.speed.forEach(s -> System.out.print(s + "; "));
                            System.out.println();
                        }
                    }
                }));*/

        /*
        * .apply("Print", ParDo.of(new DoFn<Data, Void>() {
                    @ProcessElement
                    public void processElement(@Element Data input, OutputReceiver<Void> out) {
                        System.out.println(input.id);
                        input.speed.forEach(s -> System.out.println(s));
                    }
                }));*/
         /*   p.apply(ParDo.of(new DoFn<String, Data>() {
            @ProcessElement
            public Data filterMinus(@Element Data input, OutputReceiver<Void> out) {

            }
        }));*/

        /*.apply("Print", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(@Element String input, OutputReceiver<Void> out) {
                        System.out.println(input);
                    }
                }));*/
/*        p.run().waitUntilFinish();
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
}*/