package org.example;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TestGeneratorTest {

    @Test
    void generateTestData() {
        String result = TestGenerator.generateTestData();
        assertNotNull(result);
        String[] split = result.split(" ");
        assertTrue(split.length >= 2);
    }

    @Test
    void getRandomDelay() {
        int delay = TestGenerator.getRandomDelay();
        assertTrue(delay >= TestGenerator.m1 && delay <= TestGenerator.m2);
    }

    @Test
    void randomMeasurementCount() {
        int n = TestGenerator.randomMeasurementCount();
        assertTrue(n >= 0 && n <= TestGenerator.maxMeasurementCount);
    }
}