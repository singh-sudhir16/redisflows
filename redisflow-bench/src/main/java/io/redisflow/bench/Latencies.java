package io.redisflow.bench;

import java.util.Arrays;

/** Fixed-capacity latency recorder (microseconds) with percentile reporting. */
final class Latencies {

    private final long[] samples;
    private int size;

    Latencies(int capacity) {
        this.samples = new long[capacity];
    }

    synchronized void add(long micros) {
        if (size < samples.length) {
            samples[size++] = micros;
        }
    }

    synchronized void addAll(long[] micros) {
        for (long m : micros) {
            add(m);
        }
    }

    synchronized int count() {
        return size;
    }

    /** Returns {p50, p95, p99, max} in milliseconds. */
    synchronized double[] percentilesMillis() {
        if (size == 0) {
            return new double[] {0, 0, 0, 0};
        }
        long[] sorted = Arrays.copyOf(samples, size);
        Arrays.sort(sorted);
        return new double[] {at(sorted, 0.50), at(sorted, 0.95), at(sorted, 0.99), sorted[size - 1] / 1000.0};
    }

    private static double at(long[] sorted, double quantile) {
        int index = (int) Math.ceil(quantile * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))] / 1000.0;
    }
}
