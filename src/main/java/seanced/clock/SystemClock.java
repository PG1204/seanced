package seanced.clock;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class SystemClock implements Clock {
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "seanced-clock");
                t.setDaemon(true);
                return t;
    });

    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public void scheduleOnce(long delayMillis, Runnable task) {
        scheduler.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
    }

    /** Stops the background scheduler. Call on node shutdown. */
    public void close() {
        scheduler.shutdownNow();
    }
}