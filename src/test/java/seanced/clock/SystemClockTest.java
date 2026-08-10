package seanced.clock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SystemClockTest {

    @Test
    void firesTaskWhenTimeReachesDueTime() {
        SystemClock clock = new SystemClock();
        boolean[] ran = {false};

        clock.scheduleOnce(50, () -> ran[0] = true);

        assertFalse(ran[0], "task should not run before time advances");

        clock.advanceTo(50);

        assertTrue(ran[0], "task should run once time reaches its due-time");
    }
}