package org.example.DataProcessing;

import org.example.Kafka.Producer;
import org.example.TestGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class ProcessorTest {
    static ArrayList<String> t = new ArrayList<>();
    static ArrayList<String> data = new ArrayList<>();
    Processor processor = new Processor();
    int timeframe = 30000;

    @BeforeAll
    static void set() {
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
        /*
        Sensor 1 | timeframe(s) of length: 30000ms | average speeds: 89.12 km/h (window number 0), 90.81 km/h (window number 1), 89.53 km/h (window number 2).
        Sensor 2 | timeframe(s) of length: 30000ms | average speeds: 89.58 km/h (window number 0).
        Sensor 3 | timeframe(s) of length: 30000ms | average speeds: 88.76 km/h (window number 1).
        * */
    }

    @BeforeEach
    void setUp() {
        Producer p = mock(Producer.class);

        TestGenerator test = new TestGenerator();
        doAnswer(invocation -> {
            t = invocation.getArgument(0);
            return null;
        }).when(p).sendMessageList(any());
        test.generateTestDataBatch(p, 10);
    }

    @Test
    void processData() throws ParseException {
        processor.processData(data);
        processor.results.forEach(result -> {
            assertFalse(result.speed.isEmpty());
            assertFalse(result.speed.stream().anyMatch(n -> n < 0));
        });
    }

    @Test
    void calculateAvgForEachSensorInTimeframe() throws ParseException {

        processor.processData(data);
        processor.calculateAvgForEachSensorInTimeframe(1, timeframe);
        //check if number of frames are correct
        long first = processor.results.getFirst().timestamp.getTime();
        long last = processor.results.getLast().timestamp.getTime();
        int frame = (int) ((last - first) / timeframe);
        assertEquals(frame, processor.processedData.get("1").size() - 1);
    }

    @Test
    void calculatesAvgSpeedInSection() {

    }
}