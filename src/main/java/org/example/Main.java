package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.Kafka.KafkaTopicCreator;
import org.example.Kafka.Producer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static String topic = "Measurements" + UUID.randomUUID(); //random topic for testing purposes
    static int batchSize = 1;
    static String bootstrapServers = "localhost:29092";
    static String[] sensorSeq = new String[]{"1", "2"};
    static Map<String, List<Double>> sensorsThroughTime = new HashMap<>();
    static int windowCount = 0;

    public static void main(String[] args) throws InterruptedException {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        new KafkaTopicCreator().createTopic(topic, 1);
        List<String> sensorList = Arrays.asList(sensorSeq);

        Producer producer = new Producer();

        Thread t = new Thread(() -> {
            while (true) {
                new TestGenerator().generate(producer, batchSize);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
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

        PCollection<KV<String, ArrayList<Double>>> cleanedData = collection
                .apply("FilterEmptyValues", Filter.by((SerializableFunction<String, Boolean>) input -> input.split(" ").length == 3))
                //.apply(ParDo.of(new WithTimestamp()))
                .apply("ConvertToKeyValuePairs", ParDo.of(new ToKeyValue()))
                .apply("RemoveNegativeValues", ParDo.of(new FilterNegativeValues()))
                .apply("ConvertM/SToKM/H", ParDo.of(new ConvertToKMH()));

        PCollection<KV<String, Double>> avg = cleanedData
                .apply("Windowing", Window.<KV<String, ArrayList<Double>>>into(FixedWindows.of(Duration.standardSeconds(10))).triggering(DefaultTrigger.of()))
                .apply("FlattensArray", ParDo.of(new Flat()))
                .apply(Mean.perKey())
                .apply(ParDo.of(new AddToSensorData()));

        avg.apply("Print mean", ParDo.of(new PrintAvg()));

        avg.apply("Filter Sensor-Set", Filter.by((SerializableFunction<KV<String, Double>, Boolean>) input -> sensorList.contains(input.getKey())))
                .apply(Group.globally())
                .apply("Print Sensor Sequence", ParDo.of(new PrintSequence()))
                .apply("Print all avg for sensor", ParDo.of(new PrintMap()));


        p.run().waitUntilFinish();
    }

    static class PrintMap extends DoFn<Iterable<KV<String, Double>>, Iterable<KV<String, Double>>> {
        @ProcessElement
        public void processElement() {
            windowCount++;
            for (Map.Entry<String, List<Double>> entry : sensorsThroughTime.entrySet()) {
                String sensorName = entry.getKey();
                List<Double> avgSpeeds = entry.getValue();
                // an vorletzter Stelle einfügen
                int indexToInsert = avgSpeeds.size() - 1;
                // fill up with zero if sensor had no value in a time window
                while (avgSpeeds.size() < windowCount) {
                    avgSpeeds.add(indexToInsert, 0.0);
                    sensorsThroughTime.put(sensorName, avgSpeeds);
                }
                System.out.println("Review | Sensor: " + entry.getKey() + " | AvgSpeeds: "
                        + entry.getValue().stream().map(speed -> String.format(Locale.US, "%.3f km/h", speed))
                        .collect(Collectors.joining(", ")));
            }
        }
    }

    static class Flat extends DoFn<KV<String, ArrayList<Double>>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.element().getValue().forEach(e -> c.output(KV.of(c.element().getKey(), e)));
        }
    }

    static class PrintAvg extends DoFn<KV<String, Double>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element().getKey() + " has an avg speed of: " + String.format(Locale.US, "%.3fkm/h", c.element().getValue()));
        }
    }

    static class PrintSequence extends DoFn<Iterable<KV<String, Double>>, Iterable<KV<String, Double>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            StringBuilder builder = new StringBuilder("Sensor Sequence " + Arrays.toString(sensorSeq) + " | in the current time window | ");
            c.element().forEach(kv -> builder.append("Sensor ").append(kv.getKey()).append(String.format(Locale.US, ": %.2f km/h; ", kv.getValue())));
            System.out.println(builder);
            c.output(c.element());
        }
    }

    static class AddToSensorData extends DoFn<KV<String, Double>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String name = c.element().getKey();
            double avgSpeed = c.element().getValue();
            if (sensorsThroughTime.containsKey(name)) {
                sensorsThroughTime.get(name).add(avgSpeed);
            } else {
                List<Double> averageSpeedList = new ArrayList<>();
                averageSpeedList.add(avgSpeed);
                sensorsThroughTime.put(name, averageSpeedList);
            }
            c.output(c.element());
        }
    }

    static class WithTimestamp extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window, PaneInfo pane) {
            String timestamp = c.element().split(" ")[0];
            c.outputWithTimestamp(c.element(), Instant.parse(timestamp).plus(Duration.standardSeconds(1)));
        }
    }

    static class ToKeyValue extends DoFn<String, KV<String, ArrayList<Double>>> {
        @ProcessElement
        public void processElement(@Element String input, OutputReceiver<KV<String, ArrayList<Double>>> out) {
            String[] split = input.split(" ");
            ArrayList<Double> speeds = new ArrayList<>();
            Arrays.stream(split[2].split(",")).map(Double::parseDouble).forEach(speeds::add);
            out.output(KV.of(split[1], speeds));
        }
    }

    static class FilterNegativeValues extends DoFn<KV<String, ArrayList<Double>>, KV<String, ArrayList<Double>>> {
        @ProcessElement
        public void processElement(@Element KV<String, ArrayList<Double>> input, OutputReceiver<KV<String, ArrayList<Double>>> out) {
            ArrayList<Double> filteredList = new ArrayList<>();
            input.getValue().stream().filter(value -> value >= 0).forEach(filteredList::add);
            if (!filteredList.isEmpty()) {
                out.output(KV.of(input.getKey(), filteredList));
            }
        }
    }

    static class ConvertToKMH extends DoFn<KV<String, ArrayList<Double>>, KV<String, ArrayList<Double>>> {
        @ProcessElement
        public void processElement(@Element KV<String, ArrayList<Double>> input, OutputReceiver<KV<String, ArrayList<Double>>> out) {
            ArrayList<Double> filteredList = new ArrayList<>();
            input.getValue().stream().map(speed -> speed * 3.6).forEach(filteredList::add);
            if (!filteredList.isEmpty()) {
                out.output(KV.of(input.getKey(), filteredList));
            }
        }
    }
}
