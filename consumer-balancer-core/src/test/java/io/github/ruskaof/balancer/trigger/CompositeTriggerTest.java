package io.github.ruskaof.balancer.trigger;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CompositeTriggerTest {

    @Test
    void allModeRequiresEveryTrigger() {
        assertTrue(new CompositeTrigger(List.of(() -> true, () -> true), CompositeTrigger.Mode.ALL)
                .shouldTrigger());
        assertFalse(new CompositeTrigger(List.of(() -> true, () -> false), CompositeTrigger.Mode.ALL)
                .shouldTrigger());
    }

    @Test
    void anyModeRequiresOneTrigger() {
        assertTrue(new CompositeTrigger(List.of(() -> false, () -> true), CompositeTrigger.Mode.ANY)
                .shouldTrigger());
        assertFalse(new CompositeTrigger(List.of(() -> false, () -> false), CompositeTrigger.Mode.ANY)
                .shouldTrigger());
    }

    @Test
    void evaluatesEveryTriggerWithoutShortCircuiting() {
        AtomicInteger secondTriggerCalls = new AtomicInteger();
        RebalanceTrigger counting = () -> {
            secondTriggerCalls.incrementAndGet();
            return true;
        };

        new CompositeTrigger(List.of(() -> false, counting), CompositeTrigger.Mode.ALL).shouldTrigger();
        new CompositeTrigger(List.of(() -> true, counting), CompositeTrigger.Mode.ANY).shouldTrigger();

        assertEquals(2, secondTriggerCalls.get(),
                "stateful triggers must observe every evaluation cycle");
    }

    @Test
    void rejectsEmptyTriggerList() {
        assertThrows(IllegalArgumentException.class,
                () -> new CompositeTrigger(List.of(), CompositeTrigger.Mode.ALL));
    }
}
