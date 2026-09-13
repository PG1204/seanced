package seanced.clock;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The real clock: wall-time plus a background scheduler.
 *
 * <p>Used in production, where {@link FakeClock} is used in tests. Everything
 * above this class is written against {@link Clock}, so the protocol logic is
 * identical either way.
 *
 * <p>{@link #nowMillis()} is derived from {@link System#nanoTime()} rather than
 * {@code currentTimeMillis()} so that timeout arithmetic is immune to NTP steps
 * and other wall-clock jumps. The absolute value is meaningless; only differences
 * between readings matter, which is all the protocol uses it for.
 *
 * <p>Scheduled tasks run on a single daemon thread, so callbacks are serialised
 * with respect to each other but not with respect to your own threads.
 */
public final class SystemClock implements Clock, AutoCloseable {

    private final long originNanos = System.nanoTime();
    private final ScheduledExecutorService scheduler;

    public SystemClock() {
        this(Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "seanced-clock");
            thread.setDaemon(true);
            return thread;
        }));
    }

    public SystemClock(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public long nowMillis() {
        return (System.nanoTime() - originNanos) / 1_000_000L;
    }

    @Override
    public void scheduleOnce(long delayMillis, Runnable task) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("delayMillis must not be negative: " + delayMillis);
        }
        if (scheduler.isShutdown()) {
            return;
        }
        scheduler.schedule(guarded(task), delayMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * A task that throws would silently kill nothing but itself — but it would also
     * never be logged, because {@code ScheduledExecutorService} swallows exceptions
     * into the returned future that nobody holds. Catch and report instead.
     */
    private static Runnable guarded(Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (RuntimeException | Error e) {
                System.getLogger(SystemClock.class.getName())
                        .log(System.Logger.Level.ERROR, "scheduled task failed", e);
            }
        };
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
