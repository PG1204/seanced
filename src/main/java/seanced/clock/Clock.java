package seanced.clock;

public interface Clock {
    /** Current time in milliseconds. Monotonic-ish; used for timeouts and scheduling. */
    long nowMillis();

    /** Run {@code task} once after {@code delayMillis} has elapsed. */
    void scheduleOnce(long delayMillis, Runnable task);
}
