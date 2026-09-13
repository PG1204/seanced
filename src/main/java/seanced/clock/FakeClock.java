package seanced.clock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * A clock whose time only moves when a test tells it to.
 *
 * <p>This is a discrete-event simulator. Nothing runs in the background and no
 * thread ever sleeps: scheduled tasks sit in a queue until {@link #advanceTo}
 * or {@link #advanceBy} pushes time past their due-time, at which point they run
 * inline, in due-time order, on the calling thread.
 *
 * <p>That property is what makes the SWIM protocol testable. Probe timeouts,
 * suspicion windows and protocol periods all become exact rather than flaky, and
 * a whole cluster's worth of behaviour can be driven deterministically from a
 * single thread.
 *
 * <p>Advancing runs to a fixed point: a task that schedules further work due at
 * or before the new time will see that work run in the same call. Not thread-safe,
 * by design — tests are single-threaded.
 */
public final class FakeClock implements Clock {

    private final PriorityQueue<Pending> pending = new PriorityQueue<>(
            Comparator.comparingLong(Pending::dueTime).thenComparingLong(Pending::sequence));

    private long currentTime;
    private long sequence;

    public FakeClock() {
        this(0L);
    }

    public FakeClock(long startMillis) {
        this.currentTime = startMillis;
    }

    @Override
    public long nowMillis() {
        return currentTime;
    }

    @Override
    public void scheduleOnce(long delayMillis, Runnable task) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("delayMillis must not be negative: " + delayMillis);
        }
        pending.add(new Pending(currentTime + delayMillis, sequence++, task));
    }

    /** Moves time forward by {@code deltaMillis}, running everything that comes due. */
    public void advanceBy(long deltaMillis) {
        advanceTo(currentTime + deltaMillis);
    }

    /**
     * Moves time to {@code newTime}, running every task that comes due, earliest first.
     *
     * <p>Tasks scheduled <em>while</em> advancing are picked up in the same call if
     * they are already due, so a chain of zero-delay continuations settles completely
     * before this method returns. Time never moves backwards.
     */
    public void advanceTo(long newTime) {
        if (newTime < currentTime) {
            throw new IllegalArgumentException(
                    "clock must not move backwards: " + currentTime + " -> " + newTime);
        }
        // Step the clock to each task's own due-time before running it. Jumping straight
        // to newTime would make every task observe the end of the window instead of its
        // own instant, so anything that reschedules relative to now() would land a full
        // window late — and a simulated protocol would never make progress.
        while (true) {
            Pending next = pending.peek();
            if (next == null || next.dueTime() > newTime) {
                break;
            }
            pending.poll();
            currentTime = Math.max(currentTime, next.dueTime());
            next.task().run();
        }
        currentTime = newTime;
    }

    /**
     * Runs every remaining task regardless of due-time, jumping the clock to the
     * last one. Useful for draining at the end of a test.
     */
    public void drain() {
        while (!pending.isEmpty()) {
            Pending next = pending.poll();
            currentTime = Math.max(currentTime, next.dueTime());
            next.task().run();
        }
    }

    /** How many tasks are still waiting. */
    public int pendingCount() {
        return pending.size();
    }

    /** Due-times of everything still queued, earliest first. For debugging a stuck simulation. */
    public List<Long> pendingDueTimes() {
        List<Pending> sorted = new ArrayList<>(pending);
        sorted.sort(Comparator.comparingLong(Pending::dueTime).thenComparingLong(Pending::sequence));
        return sorted.stream().map(Pending::dueTime).toList();
    }

    /** {@code sequence} breaks due-time ties in insertion order, keeping runs reproducible. */
    private record Pending(long dueTime, long sequence, Runnable task) {
    }
}
