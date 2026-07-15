package io.github.ruskaof.balancer.weight;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

class KafkaOffsetRateWeightServiceTest {

    private static final TopicPartition T0 = new TopicPartition("t", 0);
    private static final TopicPartition T1 = new TopicPartition("t", 1);

    private final Admin admin = mock(Admin.class);
    private final AtomicLong nanoTime = new AtomicLong();
    // A sample interval no test waits out, so only explicit calls mutate the history.
    private final KafkaOffsetRateWeightService service = new KafkaOffsetRateWeightService(
            admin, Duration.ofSeconds(60), Duration.ofHours(1), false, nanoTime::get);

    @AfterEach
    void closeService() {
        service.close();
        verify(admin, never()).close();
        verify(admin, never()).close(any(Duration.class));
    }

    @Test
    void firstCallHasNoBaselineAndReturnsNoRates() {
        stubEndOffsets(Map.of(T0, 100L, T1, 200L));

        assertEquals(Map.of(), service.computeWeights(Set.of(T0, T1)));
    }

    @Test
    void secondCallReturnsEventsPerSecondSinceTheFirst() {
        stubEndOffsets(Map.of(T0, 100L, T1, 200L));
        service.computeWeights(Set.of(T0, T1));

        advanceSeconds(60);
        stubEndOffsets(Map.of(T0, 700L, T1, 200L));

        assertEquals(
                Map.of(T0, 10.0, T1, 0.0),
                service.computeWeights(Set.of(T0, T1)));
    }

    @Test
    void usesTheYoungestSnapshotOlderThanTheRateInterval() {
        stubEndOffsets(Map.of(T0, 0L));
        service.computeWeights(Set.of(T0));           // t=0s, offset 0

        advanceSeconds(30);
        stubEndOffsets(Map.of(T0, 3_000L));
        service.computeWeights(Set.of(T0));           // t=30s, offset 3000

        advanceSeconds(60);
        stubEndOffsets(Map.of(T0, 3_600L));

        // t=90s: the t=30s snapshot is the youngest one at least 60s old, so the rate
        // covers 60s and 600 events — not the 90s/3600 events since t=0.
        assertEquals(Map.of(T0, 10.0), service.computeWeights(Set.of(T0)));
    }

    @Test
    void backgroundSamplesFeedTheBaseline() {
        stubEndOffsets(Map.of(T0, 100L));
        service.sampleQuietly(); // no partitions requested yet, must be a no-op
        service.computeWeights(Set.of(T0));

        advanceSeconds(60);
        stubEndOffsets(Map.of(T0, 1_300L));
        service.sampleQuietly();

        advanceSeconds(60);
        stubEndOffsets(Map.of(T0, 1_900L));

        assertEquals(Map.of(T0, 10.0), service.computeWeights(Set.of(T0)));
    }

    @Test
    void partitionsWithoutBaselineOffsetFallOutOfTheResult() {
        stubEndOffsets(Map.of(T0, 100L));
        service.computeWeights(Set.of(T0));

        advanceSeconds(60);
        stubEndOffsets(Map.of(T0, 700L, T1, 500L)); // T1 appeared after the baseline

        assertEquals(Map.of(T0, 10.0), service.computeWeights(Set.of(T0, T1)));
    }

    @Test
    void backwardsEndOffsetsFallOutOfTheResult() {
        stubEndOffsets(Map.of(T0, 100L, T1, 900L));
        service.computeWeights(Set.of(T0, T1));

        advanceSeconds(60);
        stubEndOffsets(Map.of(T0, 700L, T1, 300L)); // T1 recreated: offset went backwards

        assertEquals(Map.of(T0, 10.0), service.computeWeights(Set.of(T0, T1)));
    }

    @Test
    void emptyPartitionSetDoesNotTouchTheAdminClient() {
        assertEquals(Map.of(), service.computeWeights(Set.of()));
        verifyNoInteractions(admin);
    }

    @Test
    void failedListOffsetsThrowsIllegalState() {
        KafkaFutureImpl<Map<TopicPartition, ListOffsetsResultInfo>> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new RuntimeException("boom"));
        ListOffsetsResult result = mock(ListOffsetsResult.class);
        when(result.all()).thenReturn(failed);
        when(admin.listOffsets(anyMap())).thenReturn(result);

        assertThrows(IllegalStateException.class, () -> service.computeWeights(Set.of(T0)));
    }

    @Test
    void failedBackgroundSampleIsSwallowedAndDoesNotPoisonTheHistory() {
        stubEndOffsets(Map.of(T0, 100L));
        service.computeWeights(Set.of(T0));

        advanceSeconds(30);
        KafkaFutureImpl<Map<TopicPartition, ListOffsetsResultInfo>> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new RuntimeException("boom"));
        ListOffsetsResult failedResult = mock(ListOffsetsResult.class);
        when(failedResult.all()).thenReturn(failed);
        when(admin.listOffsets(anyMap())).thenReturn(failedResult);
        assertDoesNotThrow(service::sampleQuietly);

        advanceSeconds(30);
        stubEndOffsets(Map.of(T0, 700L));

        assertEquals(Map.of(T0, 10.0), service.computeWeights(Set.of(T0)));
    }

    @Test
    void rejectsSubSecondIntervals() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaOffsetRateWeightService(admin, Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaOffsetRateWeightService(admin, Duration.ofMinutes(1), Duration.ofMillis(500)));
    }

    @Test
    void derivesTheSampleIntervalFromTheRateInterval() {
        try (var s = new KafkaOffsetRateWeightService(admin, Duration.ofMinutes(1))) {
            assertEquals(Duration.ofSeconds(15), s.getSampleInterval());
        }
        try (var s = new KafkaOffsetRateWeightService(admin, Duration.ofSeconds(2))) {
            assertEquals(Duration.ofSeconds(1), s.getSampleInterval());
        }
        try (var s = new KafkaOffsetRateWeightService(admin, Duration.ofHours(1))) {
            assertEquals(Duration.ofSeconds(30), s.getSampleInterval());
        }
    }

    private void advanceSeconds(long seconds) {
        nanoTime.addAndGet(Duration.ofSeconds(seconds).toNanos());
    }

    private void stubEndOffsets(Map<TopicPartition, Long> endOffsets) {
        Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new HashMap<>();
        endOffsets.forEach((tp, offset) -> futures.put(tp, KafkaFuture.completedFuture(
                new ListOffsetsResultInfo(offset, 0L, Optional.empty()))));
        when(admin.listOffsets(anyMap())).thenReturn(new ListOffsetsResult(futures));
    }
}
