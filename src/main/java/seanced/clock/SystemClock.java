package seanced.clock;
import java.util.ArrayList;
import java.util.List;

public final class SystemClock implements Clock {
    private long currentTime = 0;
    private final List<Pending> pendingList = new ArrayList<>();

    @Override
    public long nowMillis() {
        return currentTime;
    }

    @Override
    public void scheduleOnce(long delayMillis, Runnable task) {
        long dueTime = currentTime + delayMillis;
        pendingList.add(new Pending(dueTime, task));
    }

    private record Pending(long dueTime, Runnable task) {}

    public void advanceTo(long newTime) {
        currentTime = newTime;
        List<Pending> tasksToBeRun = new ArrayList<>();

        for (Pending taskToBeRun: pendingList) {
            if (taskToBeRun.dueTime() <= currentTime) tasksToBeRun.add(taskToBeRun);
        }

        pendingList.removeAll(tasksToBeRun);

        for (Pending taskToBeRun : tasksToBeRun) {
            taskToBeRun.task().run();
        }
    }
}