package io.github.ruskaof.balancer.weight;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Weighs partitions by their events/sec produce rate, measured by tracking partition end
 * offsets through Kafka's {@link Admin} client — no external metrics backend needed.
 *
 * <p>End offsets are snapshotted on every {@link #computeWeights(Set)} call and, once the
 * partition set is known, by a background daemon thread every {@code sampleInterval}. A
 * partition's weight is the offset growth between the freshest snapshot and a baseline
 * snapshot about {@code rateInterval} old, divided by the elapsed seconds.
 *
 * <p>Until two snapshots at least {@value #MIN_RATE_WINDOW_MILLIS}&nbsp;ms apart exist —
 * e.g. on the very first call after startup — no rates can be computed and partitions are
 * left out of the result, so callers fall back to {@link PartitionWeightDefaults#MISSING}.
 * Partitions whose end offset went backwards (topic recreated) are left out the same way.
 *
 * <p>Instances own a sampler thread and must be {@link #close() closed}. The
 * {@link Admin} client is closed too only when created via
 * {@link #withOwnAdminClient(Map, Duration, Duration)}.
 */
@Slf4j
public class KafkaOffsetRateWeightService implements WeightService, AutoCloseable {

    public static final Duration DEFAULT_RATE_INTERVAL = Duration.ofMinutes(1);

    private static final Duration MIN_INTERVAL = Duration.ofSeconds(1);
    private static final Duration MAX_DERIVED_SAMPLE_INTERVAL = Duration.ofSeconds(30);
    private static final long LIST_OFFSETS_TIMEOUT_MS = 30_000L;
    private static final long MIN_RATE_WINDOW_MILLIS = 500L;

    private final Admin adminClient;
    private final boolean closeAdminClientOnClose;
    private final Duration rateInterval;
    private final Duration sampleInterval;
    private final LongSupplier nanoTime;
    private final ScheduledExecutorService sampler;

    private final Object lock = new Object();
    private final Deque<OffsetsSnapshot> history = new ArrayDeque<>(); // guarded by lock
    private Set<TopicPartition> trackedPartitions = Set.of();          // guarded by lock
    private final AtomicLong sampleErrors = new AtomicLong();          // sampler thread writes, any thread reads

    /**
     * Uses a sample interval derived from the rate interval: a quarter of it, clamped
     * between 1 and 30 seconds.
     *
     * @param adminClient shared client; not closed by this service
     * @param rateInterval window over which end-offset growth is turned into events/sec
     */
    public KafkaOffsetRateWeightService(Admin adminClient, Duration rateInterval) {
        this(adminClient, rateInterval, null, false, System::nanoTime);
    }

    /**
     * @param adminClient    shared client; not closed by this service
     * @param rateInterval   window over which end-offset growth is turned into events/sec
     * @param sampleInterval how often the background thread snapshots end offsets
     */
    public KafkaOffsetRateWeightService(Admin adminClient, Duration rateInterval, Duration sampleInterval) {
        this(adminClient, rateInterval,
                Objects.requireNonNull(sampleInterval, "sampleInterval"),
                false, System::nanoTime);
    }

    /**
     * Creates a service with its own {@link Admin} client built from
     * {@code adminClientConfigs}; the client is closed together with the service.
     *
     * @param sampleInterval how often end offsets are snapshotted, or {@code null} to
     *                       derive it from the rate interval
     */
    public static KafkaOffsetRateWeightService withOwnAdminClient(
            Map<String, Object> adminClientConfigs,
            Duration rateInterval,
            Duration sampleInterval) {
        return new KafkaOffsetRateWeightService(
                Admin.create(adminClientConfigs), rateInterval, sampleInterval, true, System::nanoTime);
    }

    KafkaOffsetRateWeightService(
            Admin adminClient,
            Duration rateInterval,
            Duration sampleInterval,
            boolean closeAdminClientOnClose,
            LongSupplier nanoTime) {
        this.adminClient = Objects.requireNonNull(adminClient, "adminClient");
        this.rateInterval = requireAtLeastMinInterval(rateInterval, "rateInterval");
        this.sampleInterval = sampleInterval == null
                ? deriveSampleInterval(this.rateInterval)
                : requireAtLeastMinInterval(sampleInterval, "sampleInterval");
        this.closeAdminClientOnClose = closeAdminClientOnClose;
        this.nanoTime = nanoTime;
        this.sampler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "consumer-balancer-offset-rate-sampler");
            thread.setDaemon(true);
            return thread;
        });
        this.sampler.scheduleWithFixedDelay(
                this::sampleQuietly,
                this.sampleInterval.toNanos(),
                this.sampleInterval.toNanos(),
                TimeUnit.NANOSECONDS);
    }

    public Duration getRateInterval() {
        return rateInterval;
    }

    public Duration getSampleInterval() {
        return sampleInterval;
    }

    /** Background end-offset samples that have failed since this service was created. */
    public long getSampleErrors() {
        return sampleErrors.get();
    }

    /** Partitions the background sampler currently snapshots; safe to call from any thread. */
    public int getTrackedPartitionCount() {
        synchronized (lock) {
            return trackedPartitions.size();
        }
    }

    @Override
    public Map<TopicPartition, Double> computeWeights(Set<TopicPartition> allPartitions) {
        if (allPartitions.isEmpty()) {
            return Map.of();
        }
        synchronized (lock) {
            trackedPartitions = Set.copyOf(allPartitions);
        }

        Map<TopicPartition, Long> endOffsets = fetchEndOffsets(allPartitions);
        OffsetsSnapshot current = new OffsetsSnapshot(nanoTime.getAsLong(), endOffsets);

        final OffsetsSnapshot baseline;
        synchronized (lock) {
            baseline = baselineFor(current.nanoTime());
            history.addLast(current);
            prune(current.nanoTime());
        }
        return ratesBetween(baseline, current, allPartitions);
    }

    @Override
    public void close() {
        sampler.shutdownNow();
        if (closeAdminClientOnClose) {
            adminClient.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Background snapshot of the most recently requested partitions. Never throws: a
     * failed sample only means rates are computed from an older baseline.
     * Package-visible for tests.
     */
    void sampleQuietly() {
        final Set<TopicPartition> tracked;
        synchronized (lock) {
            tracked = trackedPartitions;
        }
        if (tracked.isEmpty()) {
            return;
        }
        try {
            Map<TopicPartition, Long> endOffsets = fetchEndOffsets(tracked);
            long now = nanoTime.getAsLong();
            synchronized (lock) {
                history.addLast(new OffsetsSnapshot(now, endOffsets));
                prune(now);
            }
        } catch (Exception e) {
            sampleErrors.incrementAndGet();
            log.warn("Background end-offset sample failed; weights will use an older baseline", e);
        }
    }

    private Map<TopicPartition, Long> fetchEndOffsets(Set<TopicPartition> partitions) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition tp : partitions) {
            request.put(tp, OffsetSpec.latest());
        }
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> infos =
                    adminClient.listOffsets(request).all().get(LIST_OFFSETS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            infos.forEach((tp, info) -> endOffsets.put(tp, info.offset()));
            return endOffsets;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while listing Kafka end offsets", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException(
                    "Failed to list Kafka end offsets for partition weights. partitions=" + partitions, e);
        }
    }

    /**
     * The youngest snapshot at least {@code rateInterval} old, or the oldest snapshot
     * when none has aged that much yet; {@code null} while the history is empty.
     */
    private OffsetsSnapshot baselineFor(long now) {
        Iterator<OffsetsSnapshot> newestFirst = history.descendingIterator();
        while (newestFirst.hasNext()) {
            OffsetsSnapshot snapshot = newestFirst.next();
            if (now - snapshot.nanoTime() >= rateInterval.toNanos()) {
                return snapshot;
            }
        }
        return history.peekFirst();
    }

    /**
     * Drops snapshots that can no longer become a baseline: everything older than the
     * youngest snapshot that already exceeds {@code rateInterval}.
     */
    private void prune(long now) {
        while (history.size() >= 2) {
            Iterator<OffsetsSnapshot> oldestFirst = history.iterator();
            oldestFirst.next();
            OffsetsSnapshot secondOldest = oldestFirst.next();
            if (now - secondOldest.nanoTime() >= rateInterval.toNanos()) {
                history.removeFirst();
            } else {
                return;
            }
        }
    }

    private Map<TopicPartition, Double> ratesBetween(
            OffsetsSnapshot baseline,
            OffsetsSnapshot current,
            Set<TopicPartition> partitions) {
        if (baseline == null) {
            log.warn("No end-offset history yet; all {} partitions fall back to the default weight {}",
                    partitions.size(), PartitionWeightDefaults.MISSING);
            return Map.of();
        }
        long elapsedNanos = current.nanoTime() - baseline.nanoTime();
        if (elapsedNanos < TimeUnit.MILLISECONDS.toNanos(MIN_RATE_WINDOW_MILLIS)) {
            log.warn("End-offset history spans only {} ms; all {} partitions fall back to the default weight {}",
                    TimeUnit.NANOSECONDS.toMillis(elapsedNanos), partitions.size(), PartitionWeightDefaults.MISSING);
            return Map.of();
        }
        double elapsedSeconds = elapsedNanos / 1e9;

        Map<TopicPartition, Double> weights = new HashMap<>();
        for (TopicPartition tp : partitions) {
            Long currentOffset = current.endOffsets().get(tp);
            Long baselineOffset = baseline.endOffsets().get(tp);
            if (currentOffset == null || baselineOffset == null) {
                log.debug("No baseline end offset for {} yet; it falls back to the default weight", tp);
                continue;
            }
            long delta = currentOffset - baselineOffset;
            if (delta < 0) {
                log.warn("End offset of {} went backwards ({} -> {}), topic recreated?"
                        + " It falls back to the default weight", tp, baselineOffset, currentOffset);
                continue;
            }
            weights.put(tp, delta / elapsedSeconds);
        }
        log.debug("Computed weights over a {} ms window: {}", TimeUnit.NANOSECONDS.toMillis(elapsedNanos), weights);
        return weights;
    }

    private static Duration deriveSampleInterval(Duration rateInterval) {
        Duration derived = rateInterval.dividedBy(4);
        if (derived.compareTo(MIN_INTERVAL) < 0) {
            return MIN_INTERVAL;
        }
        if (derived.compareTo(MAX_DERIVED_SAMPLE_INTERVAL) > 0) {
            return MAX_DERIVED_SAMPLE_INTERVAL;
        }
        return derived;
    }

    private static Duration requireAtLeastMinInterval(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.compareTo(MIN_INTERVAL) < 0) {
            throw new IllegalArgumentException(
                    name + " must be at least " + MIN_INTERVAL.toMillis() + " ms, but was " + value);
        }
        return value;
    }

    private record OffsetsSnapshot(long nanoTime, Map<TopicPartition, Long> endOffsets) {
    }
}
