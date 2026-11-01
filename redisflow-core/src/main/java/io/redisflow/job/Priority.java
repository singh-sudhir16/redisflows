package io.redisflow.job;

/** Priority levels (lower runs first). Levels at or above {@link #HIGH} bypass the FIFO streams. */
public final class Priority {

    public static final int CRITICAL = 1;
    public static final int HIGH = 2;
    public static final int NORMAL = 3;
    public static final int LOW = 4;

    /** Jobs with priority <= this value go to the priority set instead of their stream. */
    public static final int PRIORITY_LANE_THRESHOLD = HIGH;

    private Priority() {
    }

    public static boolean usesPriorityLane(int priority) {
        return priority <= PRIORITY_LANE_THRESHOLD;
    }
}
