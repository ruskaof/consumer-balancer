package io.github.ruskaof.balancer.weight;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * <p>End offsets are sampled on every {@link #computeWeights(Set)} call and by a background
 * daemon thread every {@code sampleInterval}. A partition's weight is the offset growth
 * between its freshest sample and a baseline sample about {@code rateInterval} old, divided
 * by the elapsed seconds.
 *
 * <p>One instance serves any number of consumer groups — e.g. every group of an
 * {@link io.github.ruskaof.balancer.ConsumerGroupBalancers} registry. Every partition keeps
 * its own sample history, and the background thread samples every partition ever requested,
 * so computing one group's weights never costs another group its baselines. A partition only
 * stops being sampled after {@value #MAX_CONSECUTIVE_SAMPLE_FAILURES} background samples in a
 * row failed for it while other partitions succeeded — typically a deleted topic; requesting
 * it again resumes sampling. Partitions are deliberately not expired by age: the group leader
 * may only ask for weights once per rebalance, and it needs a warm history when it does.
 *
 * <p>Until a partition has two samples at least {@value #MIN_RATE_WINDOW_MILLIS}&nbsp;ms apart
 * — e.g. on the very first call after startup — no rate can be computed for it and it is left
 * out of the result, so callers fall back to {@link PartitionWeightDefaults#MISSING}.
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
    private static final int MAX_CONSECUTIVE_SAMPLE_FAILURES = 3;

    private final Admin adminClient;
    private final boolean closeAdminClientOnClose;
    private final Duration rateInterval;
    private final Duration sampleInterval;
    private final LongSupplier nanoTime;
    private final ScheduledExecutorService sampler;

    private final Object lock = new Object();
    private final Map<TopicPartition, PartitionHistory> histories = new HashMap<>(); // guarded by lock
    private final AtomicLong sampleErrors = new AtomicLong(); // sampler thread writes, any thread reads

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
     * @param sampleInterval how often the background thread samples end offsets
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
     * @param sampleInterval how often end offsets are sampled, or {@code null} to
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

    /** Background samples in which at least one partition failed, since this service was created. */
    public long getSampleErrors() {
        return sampleErrors.get();
    }

    /** Partitions the background sampler currently samples; safe to call from any thread. */
    public int getTrackedPartitionCount() {
        synchronized (lock) {
            return histories.size();
        }
    }

    @Override
    public Map<TopicPartition, Double> computeWeights(Set<TopicPartition> allPartitions) {
        if (allPartitions.isEmpty()) {
            return Map.of();
        }
        synchronized (lock) {
            for (TopicPartition tp : allPartitions) {
                histories.computeIfAbsent(tp, k -> new PartitionHistory());
            }
        }

        Fetch fetch = fetchEndOffsets(allPartitions);
        if (!fetch.failures().isEmpty()) {
            throw new IllegalStateException(
                    "Failed to list Kafka end offsets for partition weights. partitions="
                            + fetch.failures().keySet(),
                    fetch.firstFailure());
        }

        Map<TopicPartition, OffsetSample> baselines = new HashMap<>();
        synchronized (lock) {
            fetch.samples().forEach((tp, current) -> {
                // Re-created if the sampler dropped the partition in the meantime.
                PartitionHistory history = histories.computeIfAbsent(tp, k -> new PartitionHistory());
                OffsetSample baseline = history.baselineFor(current.nanoTime());
                if (baseline != null) {
                    baselines.put(tp, baseline);
                }
                history.add(current);
            });
        }
        return ratesBetween(baselines, fetch.samples(), allPartitions);
    }

    @Override
    public void close() {
        sampler.shutdownNow();
        if (closeAdminClientOnClose) {
            adminClient.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Background sample of every tracked partition. Never throws: a failed sample only means
     * rates are computed from an older baseline. Package-visible for tests.
     */
    void sampleQuietly() {
        final Set<TopicPartition> tracked;
        synchronized (lock) {
            tracked = Set.copyOf(histories.keySet());
        }
        if (tracked.isEmpty()) {
            return;
        }
        final Fetch fetch;
        try {
            fetch = fetchEndOffsets(tracked);
        } catch (Exception e) {
            sampleErrors.incrementAndGet();
            log.warn("Background end-offset sample failed; weights will use an older baseline", e);
            return;
        }

        List<TopicPartition> dropped = new ArrayList<>();
        synchronized (lock) {
            fetch.samples().forEach((tp, sample) -> {
                PartitionHistory history = histories.get(tp);
                if (history != null) {
                    history.add(sample);
                }
            });
            // A partition failing alone is broken (deleted topic); every partition failing
            // together is a broken cluster, which must not wipe the histories.
            boolean othersSucceeded = !fetch.samples().isEmpty();
            for (TopicPartition tp : fetch.failures().keySet()) {
                PartitionHistory history = histories.get(tp);
                if (history != null
                        && ++history.consecutiveFailures >= MAX_CONSECUTIVE_SAMPLE_FAILURES
                        && othersSucceeded) {
                    histories.remove(tp);
                    dropped.add(tp);
                }
            }
        }

        if (!fetch.failures().isEmpty()) {
            sampleErrors.incrementAndGet();
            log.warn("Background end-offset sample failed for {} of {} partitions; their weights will use an"
                    + " older baseline", fetch.failures().size(), tracked.size(), fetch.firstFailure());
        }
        if (!dropped.isEmpty()) {
            log.warn("Stopped sampling {} partition(s) that failed {} background samples in a row while other"
                            + " partitions succeeded (deleted topic?); requesting them again resumes sampling: {}",
                    dropped.size(), MAX_CONSECUTIVE_SAMPLE_FAILURES, dropped);
        }
    }

    /**
     * Lists the end offsets of {@code partitions}. Each partition is timestamped when its own
     * response arrives, so one slow partition cannot skew the elapsed time — and with it the
     * rate — of the others. Partitions still pending at the deadline count as failed.
     */
    private Fetch fetchEndOffsets(Set<TopicPartition> partitions) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition tp : partitions) {
            request.put(tp, OffsetSpec.latest());
        }
        ListOffsetsResult result = adminClient.listOffsets(request);

        Map<TopicPartition, OffsetSample> arrived = new ConcurrentHashMap<>();
        Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> recorded = new HashMap<>();
        for (TopicPartition tp : partitions) {
            // The future whenComplete returns completes only after the action ran, so once
            // it is done, the partition's sample is in the map.
            recorded.put(tp, result.partitionResult(tp).whenComplete((info, error) -> {
                if (error == null) {
                    arrived.put(tp, new OffsetSample(nanoTime.getAsLong(), info.offset()));
                }
            }));
        }

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LIST_OFFSETS_TIMEOUT_MS);
        Map<TopicPartition, OffsetSample> samples = new HashMap<>();
        Map<TopicPartition, Throwable> failures = new HashMap<>();
        for (Map.Entry<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> entry : recorded.entrySet()) {
            TopicPartition tp = entry.getKey();
            try {
                entry.getValue().get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                samples.put(tp, arrived.get(tp));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while listing Kafka end offsets", e);
            } catch (ExecutionException e) {
                failures.put(tp, e.getCause());
            } catch (TimeoutException e) {
                failures.put(tp, e);
            }
        }
        return new Fetch(samples, failures);
    }

    private Map<TopicPartition, Double> ratesBetween(
            Map<TopicPartition, OffsetSample> baselines,
            Map<TopicPartition, OffsetSample> current,
            Set<TopicPartition> partitions) {
        if (baselines.isEmpty()) {
            log.warn("No end-offset history yet; all {} partitions fall back to the default weight {}",
                    partitions.size(), PartitionWeightDefaults.MISSING);
            return Map.of();
        }

        Map<TopicPartition, Double> weights = new HashMap<>();
        int windowTooShort = 0;
        for (TopicPartition tp : partitions) {
            OffsetSample baseline = baselines.get(tp);
            OffsetSample latest = current.get(tp);
            if (baseline == null || latest == null) {
                log.debug("No baseline end offset for {} yet; it falls back to the default weight", tp);
                continue;
            }
            long elapsedNanos = latest.nanoTime() - baseline.nanoTime();
            if (elapsedNanos < TimeUnit.MILLISECONDS.toNanos(MIN_RATE_WINDOW_MILLIS)) {
                windowTooShort++;
                continue;
            }
            long delta = latest.endOffset() - baseline.endOffset();
            if (delta < 0) {
                log.warn("End offset of {} went backwards ({} -> {}), topic recreated?"
                        + " It falls back to the default weight", tp, baseline.endOffset(), latest.endOffset());
                continue;
            }
            weights.put(tp, delta / (elapsedNanos / 1e9));
        }
        if (windowTooShort > 0) {
            log.warn("End-offset history spans less than {} ms for {} of {} partitions; they fall back to the"
                            + " default weight {}",
                    MIN_RATE_WINDOW_MILLIS, windowTooShort, partitions.size(), PartitionWeightDefaults.MISSING);
        }
        log.debug("Computed weights over a {} window: {}", rateInterval, weights);
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

    /** One partition's samples, oldest first, plus its background failure streak. Guarded by {@code lock}. */
    private final class PartitionHistory {

        private final Deque<OffsetSample> samples = new ArrayDeque<>();
        private int consecutiveFailures;

        /**
         * The youngest sample at least {@code rateInterval} old, or the oldest sample when
         * none has aged that much yet; {@code null} while the history is empty.
         */
        OffsetSample baselineFor(long now) {
            Iterator<OffsetSample> newestFirst = samples.descendingIterator();
            while (newestFirst.hasNext()) {
                OffsetSample sample = newestFirst.next();
                if (now - sample.nanoTime() >= rateInterval.toNanos()) {
                    return sample;
                }
            }
            return samples.peekFirst();
        }

        void add(OffsetSample sample) {
            consecutiveFailures = 0;
            // The sampler and computeWeights race; a sample that lost the race to a newer one
            // adds nothing and would break the ordering the lookups rely on.
            OffsetSample newest = samples.peekLast();
            if (newest != null && newest.nanoTime() > sample.nanoTime()) {
                return;
            }
            samples.addLast(sample);
            prune(sample.nanoTime());
        }

        /**
         * Drops samples that can no longer become a baseline: everything older than the
         * youngest sample that already exceeds {@code rateInterval}.
         */
        private void prune(long now) {
            while (samples.size() >= 2) {
                Iterator<OffsetSample> oldestFirst = samples.iterator();
                oldestFirst.next();
                OffsetSample secondOldest = oldestFirst.next();
                if (now - secondOldest.nanoTime() >= rateInterval.toNanos()) {
                    samples.removeFirst();
                } else {
                    return;
                }
            }
        }
    }

    private record OffsetSample(long nanoTime, long endOffset) {
    }

    private record Fetch(Map<TopicPartition, OffsetSample> samples, Map<TopicPartition, Throwable> failures) {

        Throwable firstFailure() {
            return failures.values().stream().findFirst().orElse(null);
        }
    }
}
