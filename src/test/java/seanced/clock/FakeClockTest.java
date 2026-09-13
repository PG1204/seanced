package seanced.clock;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FakeClockTest {

    @Test
    void firesTaskWhenTimeReachesDueTime() {
        FakeClock clock = new FakeClock();
        boolean[] ran = {false};

        clock.scheduleOnce(50, () -> ran[0] = true);

        assertFalse(ran[0], "task should not run before time advances");

        clock.advanceTo(50);

        assertTrue(ran[0], "task should run once time reaches its due-time");
    }

    @Test
    void runsTasksInDueTimeOrderNotScheduleOrder() {
        FakeClock clock = new FakeClock();
        List<String> order = new ArrayList<>();

        clock.scheduleOnce(300, () -> order.add("third"));
        clock.scheduleOnce(100, () -> order.add("first"));
        clock.scheduleOnce(200, () -> order.add("second"));

        clock.advanceBy(500);

        assertEquals(List.of("first", "second", "third"), order);
    }

    @Test
    void runsTasksScheduledDuringAdvanceIfAlreadyDue() {
        FakeClock clock = new FakeClock();
        List<String> order = new ArrayList<>();

        // A task that schedules more work with no delay: the follow-up is due immediately,
        // so it must run inside the same advance rather than waiting for the next one.
        clock.scheduleOnce(10, () -> {
            order.add("outer");
            clock.scheduleOnce(0, () -> order.add("inner"));
        });

        clock.advanceTo(10);

        assertEquals(List.of("outer", "inner"), order);
    }

    @Test
    void doesNotRunTasksScheduledBeyondTheCurrentTime() {
        FakeClock clock = new FakeClock();
        boolean[] ran = {false};

        clock.scheduleOnce(10, () -> clock.scheduleOnce(10, () -> ran[0] = true));

        clock.advanceTo(10);
        assertFalse(ran[0], "follow-up is due at t=20 and must wait");

        clock.advanceTo(20);
        assertTrue(ran[0]);
    }

    @Test
    void tracksTimeAndPendingWork() {
        FakeClock clock = new FakeClock();

        clock.scheduleOnce(100, () -> {
        });
        clock.scheduleOnce(200, () -> {
        });

        assertEquals(0, clock.nowMillis());
        assertEquals(2, clock.pendingCount());

        clock.advanceTo(150);

        assertEquals(150, clock.nowMillis());
        assertEquals(1, clock.pendingCount());
    }

    @Test
    void drainRunsEverythingRegardlessOfDueTime() {
        FakeClock clock = new FakeClock();
        List<String> order = new ArrayList<>();

        clock.scheduleOnce(10_000, () -> order.add("late"));
        clock.scheduleOnce(5, () -> order.add("early"));

        clock.drain();

        assertEquals(List.of("early", "late"), order);
        assertEquals(10_000, clock.nowMillis());
        assertEquals(0, clock.pendingCount());
    }

    @Test
    void rejectsMovingBackwards() {
        FakeClock clock = new FakeClock();
        clock.advanceTo(100);

        assertThrows(IllegalArgumentException.class, () -> clock.advanceTo(50));
    }

    @Test
    void rejectsNegativeDelay() {
        FakeClock clock = new FakeClock();

        assertThrows(IllegalArgumentException.class, () -> clock.scheduleOnce(-1, () -> {
        }));
    }
}
