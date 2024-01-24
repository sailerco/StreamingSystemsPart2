package org.example.DataProcessing;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ProcessorTest {
    static ArrayList<String> data = new ArrayList<>();
    static Processor processor = new Processor();
    int timeframe = 30000;

    @BeforeAll
    static void set() throws ParseException {
        data.add("2023-12-31T10:28:49.685Z 2 -15.85,21.21,28.48,21.60");
        data.add("2023-12-31T10:28:57.261Z 1 27.48,22.03");
        data.add("2023-12-31T10:29:06.918Z 1 -24.44");
        data.add("2023-12-31T10:29:16.191Z 2 26.48,26.64");
        data.add("2023-12-31T10:29:23.680Z 3 25.75");
        data.add("2023-12-31T10:29:31.244Z 1 25.71,24.24,29.77");
        data.add("2023-12-31T10:29:38.667Z 1 20.95,29.59,20.93,25.38");
        data.add("2023-12-31T10:29:48.062Z 3 21.53,26.26,28.28,21.46");
        data.add("2023-12-31T10:29:55.353Z 2 ");
        data.add("2023-12-31T10:30:03.473Z 1 26.23,23.51,-10.93");
        processor.processData(data);
    }

    @Test
        //checks that no empty nor negative values are used in the data processor
    void processData() {
        processor.results.forEach(result -> {
            assertFalse(result.speed.isEmpty());
            assertFalse(result.speed.stream().anyMatch(n -> n < 0));
        });
    }

    @Test
    void calculateAvgForEachSensorInTimeframe() {
        Map<String, Map<Integer, Double>> processed = processor.calculateAvgForEachSensorInTimeframe(3, timeframe);
        assertEquals(3, processed.size());
        //check if number of frames are correct
        long first = processor.results.first().timestamp.getTime();
        long last = processor.results.last().timestamp.getTime();
        assertEquals((int) ((last - first) / timeframe), processor.avgOfProcessedData.get("1").size() - 1);
    }

    @Test
    void calculateAvgInKMH() {
        List<Double> speeds = Arrays.asList(10.0, 15.0, 20.0);
        double avgSpeed = processor.calculateAvgInKMH(speeds, true);
        assertEquals(54.0, avgSpeed);
        avgSpeed = processor.calculateAvgInKMH(speeds, false);
        assertEquals(15.0, avgSpeed);
    }
}