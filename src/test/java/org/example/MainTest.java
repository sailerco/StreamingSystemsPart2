package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class MainTest {
    @Test
    public void testFilterEmptyValues() {
        Pipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        List<String> data = Arrays.asList(
                "2024-01-22T08:14:43.230Z 3 26.69",
                "2024-01-22T08:14:51.966Z 3",
                "2024-01-22T08:14:59.981Z 1",
                "2024-01-22T08:15:08.180Z 2 26.65,27.87",
                "2024-01-22T08:15:17.714Z 1 -16.92",
                "2024-01-22T08:15:24.791Z 1 24.12,22.91");
        PCollection<String> input = p.apply(Create.of(data));
        PCollection<String> output = input.apply("FilterEmptyValues", Filter.by((SerializableFunction<String, Boolean>) i -> {
            assert i != null;
            return i.split(" ").length == 3;
        }));
        PAssert.that(output).containsInAnyOrder(
                "2024-01-22T08:14:43.230Z 3 26.69",
                "2024-01-22T08:15:08.180Z 2 26.65,27.87",
                "2024-01-22T08:15:17.714Z 1 -16.92",
                "2024-01-22T08:15:24.791Z 1 24.12,22.91");
        p.run();
    }

    @Test
    public void testFilterNegativeValues() {
        Pipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        List<KV<String, ArrayList<Double>>> data = new ArrayList<>();
        data.add(KV.of("3", new ArrayList<>(List.of(26.69))));
        data.add(KV.of("2", new ArrayList<>(Arrays.asList(26.65, 27.87))));
        data.add(KV.of("1", new ArrayList<>(List.of(-16.92))));
        data.add(KV.of("1", new ArrayList<>(Arrays.asList(24.12, 22.91))));

        PCollection<KV<String, ArrayList<Double>>> input = p.apply(Create.of(data));
        PCollection<KV<String, ArrayList<Double>>> output = input.apply("RemoveNegativeValues", ParDo.of(new Main.FilterNegativeValues()));
        PAssert.that(output).containsInAnyOrder(
                KV.of("3", new ArrayList<>(List.of(26.69))),
                KV.of("2", new ArrayList<>(Arrays.asList(26.65, 27.87))),
                KV.of("1", new ArrayList<>(Arrays.asList(24.12, 22.91))));
        p.run();
    }

    @Test
    public void testAvg() {
        Pipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        List<KV<String, Double>> data = new ArrayList<>();
        data.add(KV.of("3", 26.69));
        data.add(KV.of("2", 26.65));
        data.add(KV.of("2", 27.87));
        data.add(KV.of("1", 24.12));
        data.add(KV.of("1", 22.91));

        PCollection<KV<String, Double>> input = p.apply(Create.of(data));
        PCollection<KV<String, Double>> output = input.apply(Mean.perKey());
        PAssert.that(output).containsInAnyOrder(
                KV.of("3", 26.69),
                KV.of("2", (26.65 + 27.87)/2),
                KV.of("1", 23.515));
        p.run();
    }
}