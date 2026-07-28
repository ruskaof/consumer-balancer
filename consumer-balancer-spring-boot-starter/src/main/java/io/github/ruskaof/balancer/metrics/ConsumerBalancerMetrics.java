package io.github.ruskaof.balancer.metrics;

import io.github.ruskaof.balancer.ContainerRegistryRebalanceInitiator;
import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger;
import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Binds the balancer's meters — all prefixed {@code consumer.balancer.} and tagged with the
 * consumer group — to a registry. Every component is optional: a {@code null} component
 * simply binds no meters, so the binder adapts to whatever the application wired (e.g. a
 * custom {@code RebalanceTrigger} that is not a {@link ThresholdTrigger}).
 *
 * <p>Trigger meters read the {@link ThresholdTrigger.Status} snapshot, which only advances
 * on the instance currently elected coordinator; on all other instances they keep their
 * initial values ({@code NaN}/0). Aggregate across instances with {@code max}, or join on
 * {@code consumer.balancer.coordinator == 1}.
 *
 * <p>The auto-configuration registers one binder for the auto-configured balancer stack. An
 * application running hand-wired stacks against several Kafka clusters instantiates one
 * binder per stack, with an extra tag telling the clusters apart.
 */
public final class ConsumerBalancerMetrics implements MeterBinder {

    private final Tags tags;
    private final ThresholdTrigger trigger;
    private final CoordinatorManager coordinatorManager;
    private final ContainerRegistryRebalanceInitiator rebalanceInitiator;
    private final KafkaOffsetRateWeightService offsetRateWeightService;

    /**
     * @param tags                    common tags for every meter; include the consumer group
     * @param trigger                 may be {@code null}: skips the trigger meters
     * @param coordinatorManager      may be {@code null}: skips the coordinator gauge
     * @param rebalanceInitiator      may be {@code null}: skips the rebalance counters
     * @param offsetRateWeightService may be {@code null}: skips the offset-rate meters
     */
    public ConsumerBalancerMetrics(
            Tags tags,
            ThresholdTrigger trigger,
            CoordinatorManager coordinatorManager,
            ContainerRegistryRebalanceInitiator rebalanceInitiator,
            KafkaOffsetRateWeightService offsetRateWeightService) {
        this.tags = Objects.requireNonNull(tags, "tags");
        this.trigger = trigger;
        this.coordinatorManager = coordinatorManager;
        this.rebalanceInitiator = rebalanceInitiator;
        this.offsetRateWeightService = offsetRateWeightService;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        if (trigger != null) {
            bindTrigger(registry);
        }
        if (coordinatorManager != null) {
            bindCoordinator(registry);
        }
        if (rebalanceInitiator != null) {
            bindRebalanceInitiator(registry);
        }
        if (offsetRateWeightService != null) {
            bindOffsetRateWeightService(registry);
        }
    }

    private void bindTrigger(MeterRegistry registry) {
        Gauge.builder("consumer.balancer.trigger.imbalance.ratio", trigger, t -> t.status().lastRatio())
                .description("Max instance load divided by the optimal max instance load, from the last evaluation"
                        + " that computed it; NaN until then")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.imbalance.threshold", trigger, t -> t.status().threshold())
                .description("Configured ratio above which the trigger fires")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.instance.load", trigger, t -> t.status().lastCurrentMaxLoad())
                .description("Load carried by the most loaded instance, from the last evaluation that computed it")
                .tags(tags)
                .tag("assignment", "current")
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.instance.load", trigger, t -> t.status().lastOptimalMaxLoad())
                .description("Load the most loaded instance would carry under the optimal assignment,"
                        + " from the last evaluation that computed it")
                .tags(tags)
                .tag("assignment", "optimal")
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.members", trigger, t -> t.status().lastMemberCount())
                .description("Members of the group at the last judged evaluation")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.instances", trigger, t -> t.status().lastInstanceCount())
                .description("Application instances observed in the group at the last judged evaluation")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.partitions", trigger, t -> t.status().lastPartitionCount())
                .description("Assigned partitions in the group at the last judged evaluation")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.weights.defaulted", trigger, t -> t.status().lastDefaultedWeightCount())
                .description("Partitions whose weight fell back to the default at the last judged evaluation")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.violated.checks", trigger, t -> t.status().violatedChecks())
                .description("Consecutive checks that found the current assignment out of threshold")
                .tags(tags)
                .register(registry);
        TimeGauge.builder("consumer.balancer.trigger.cooldown", trigger, TimeUnit.MILLISECONDS,
                        t -> t.status().effectiveCooldown().toMillis())
                .description("Effective cooldown between fires, including backoff doubling")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.trigger.last.fired", trigger, t -> {
                    Instant lastFiredAt = t.status().lastFiredAt();
                    return lastFiredAt == null ? Double.NaN : (double) lastFiredAt.getEpochSecond();
                })
                .description("Epoch seconds of the last fire on this instance; NaN until the first fire")
                .baseUnit("seconds")
                .tags(tags)
                .register(registry);
        for (ThresholdTrigger.EvaluationOutcome outcome : ThresholdTrigger.EvaluationOutcome.values()) {
            FunctionCounter.builder("consumer.balancer.trigger.evaluations", trigger,
                            t -> t.status().evaluations(outcome))
                    .description("Trigger evaluations by outcome")
                    .tags(tags)
                    .tag("outcome", outcome.name().toLowerCase(Locale.ROOT))
                    .register(registry);
        }
        FunctionTimer.builder("consumer.balancer.trigger.evaluation.duration", trigger,
                        t -> t.status().evaluationCount(),
                        t -> t.status().evaluationTimeNanos(),
                        TimeUnit.NANOSECONDS)
                .description("Wall time of trigger evaluations, including the group describe and weight fetch")
                .tags(tags)
                .register(registry);
    }

    private void bindCoordinator(MeterRegistry registry) {
        Gauge.builder("consumer.balancer.coordinator", coordinatorManager, m -> m.isCoordinator() ? 1.0 : 0.0)
                .description("1 while this instance holds the group's coordinator role, 0 otherwise")
                .tags(tags)
                .register(registry);
    }

    private void bindRebalanceInitiator(MeterRegistry registry) {
        FunctionCounter.builder("consumer.balancer.rebalance.initiations", rebalanceInitiator,
                        i -> i.getInitiations() - i.getNoMatchInitiations())
                .description("Proactive rebalance initiations by whether any listener container matched")
                .tags(tags)
                .tag("result", "enforced")
                .register(registry);
        FunctionCounter.builder("consumer.balancer.rebalance.initiations", rebalanceInitiator,
                        ContainerRegistryRebalanceInitiator::getNoMatchInitiations)
                .description("Proactive rebalance initiations by whether any listener container matched")
                .tags(tags)
                .tag("result", "no_match")
                .register(registry);
        FunctionCounter.builder("consumer.balancer.rebalance.containers.enforced", rebalanceInitiator,
                        ContainerRegistryRebalanceInitiator::getContainersEnforced)
                .description("Listener containers on which a rebalance was enforced")
                .tags(tags)
                .register(registry);
    }

    private void bindOffsetRateWeightService(MeterRegistry registry) {
        FunctionCounter.builder("consumer.balancer.offset.rate.sample.errors", offsetRateWeightService,
                        KafkaOffsetRateWeightService::getSampleErrors)
                .description("Failed background end-offset samples; persistent failures degrade weights"
                        + " toward the default")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.offset.rate.tracked.partitions", offsetRateWeightService,
                        KafkaOffsetRateWeightService::getTrackedPartitionCount)
                .description("Partitions tracked by the background end-offset sampler")
                .tags(tags)
                .register(registry);
    }
}
