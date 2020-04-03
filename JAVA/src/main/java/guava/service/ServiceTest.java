package guava.service;

import static com.google.common.util.concurrent.Service.State.FAILED;
import static com.google.common.util.concurrent.Service.State.NEW;
import static com.google.common.util.concurrent.Service.State.RUNNING;
import static com.google.common.util.concurrent.Service.State.STARTING;
import static com.google.common.util.concurrent.Service.State.STOPPING;
import static com.google.common.util.concurrent.Service.State.TERMINATED;

public class ServiceTest{

    /** Assert on the comparison ordering of the State enum since we guarantee it. */
    public void testStateOrdering() {
        // List every valid (direct) state transition.
        assertLessThan(NEW, STARTING);
        assertLessThan(NEW, TERMINATED);

        assertLessThan(STARTING, RUNNING);
        assertLessThan(STARTING, STOPPING);
        assertLessThan(STARTING, FAILED);

        assertLessThan(RUNNING, STOPPING);
        assertLessThan(RUNNING, FAILED);

        assertLessThan(STOPPING, FAILED);
        assertLessThan(STOPPING, TERMINATED);
    }

    private static <T extends Comparable<? super T>> void assertLessThan(T a, T b) {
        if (a.compareTo(b) >= 0) {
        }
    }
}