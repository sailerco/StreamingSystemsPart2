package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.DataProcessing.Processor;
import org.example.Kafka.KafkaTopicCreator;
import org.example.Kafka.Producer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class Test {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    public static String topic = "Measurements" + UUID.randomUUID(); //random topic for testing purposes
    static int batchSize = 1;
    static int timeframe = 30000;
    static String bootstrapServers = "localhost:29092";
    static String[] sensorgroup = new String[]{"1", "2"};


    public static void main(String[] args) throws InterruptedException {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        new KafkaTopicCreator().createTopic(topic, 1);
        List<String> sensorList = Arrays.asList(sensorgroup);

        Producer producer = new Producer();
        Processor processor = new Processor();

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
                .apply("Windowing", Window.<KV<String, ArrayList<Double>>>into(FixedWindows.of(Duration.standardSeconds(5)))
                        .triggering(DefaultTrigger.of()))
                .apply("FlattenDoubleArray", ParDo.of(new Flat()))
                .apply(Mean.perKey())
                .apply(GroupByKey.<String, Double>create())
                .apply(ParDo.of(new CalculateMean()));
        avg.apply("Print", ParDo.of(new DoFn<KV<String, Double>, KV<String, Double>>() {
            @ProcessElement
            public void processElement(@Element KV<String, Double> input, OutputReceiver<KV<String, Double>> out) {
                System.out.println(input.getKey() + " has an avg speed of: " + input.getValue());
            }
        }));

        avg.apply("FilterSensorset", Filter.by((SerializableFunction<KV<String, Double>, Boolean>) input -> sensorList.contains(input.getKey())))
                .apply(ParDo.of(new FlatToDouble()))
                .apply(Sum.doublesGlobally().withoutDefaults())
                .apply(ParDo.of(new MeanOfSum()))
                .apply("Print", ParDo.of(new DoFn<Double, Double>() {
                    @ProcessElement
                    public void processElement(@Element Double input, OutputReceiver<Double> out) {
                        System.out.println("Avg over the current time window and given sequence " + input);
                    }
                }));

        p.run().waitUntilFinish();
    }

    static class FlatToDouble extends DoFn<KV<String, Double>, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getValue());
        }
    }

    static class MeanOfSum extends DoFn<Double, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element() / sensorgroup.length);
        }
    }

    static class CalculateMean extends DoFn<KV<String, Iterable<Double>>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            double sum = 0;
            int count = 0;
            for (Double value : c.element().getValue()) {
                sum += value;
                count++;
            }
            c.output(KV.of(c.element().getKey(), sum / count));
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

    static class Flat extends DoFn<KV<String, ArrayList<Double>>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.element().getValue().forEach(e -> c.output(KV.of(c.element().getKey(), e)));
        }
    }
}
