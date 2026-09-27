package io.github.ruskaof.balancer.metrics;

import io.github.ruskaof.balancer.ConsumerGroupBalancer;
import io.github.ruskaof.balancer.ConsumerGroupBalancers;
import io.github.ruskaof.balancer.ContainerRebalanceInitiator;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger;
import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Binds the meters of one or more {@link ConsumerGroupBalancers} registries — all prefixed
 * {@code consumer.balancer.} and tagged with the registry's {@linkplain ConsumerGroupBalancers#getTags()
 * tags} — to a meter registry:
 * <ul>
 *   <li>per registry, the offset-rate weight store meters, when its weight store is a
 *       {@link KafkaOffsetRateWeightService};</li>
 *   <li>per registered group, tagged {@code group}: the coordinator gauge, the trigger meters
 *       when its trigger is a {@link ThresholdTrigger}, and the rebalance counters when its
 *       initiator is a {@link ContainerRebalanceInitiator}. Groups registered after binding
 *       get their meters on registration.</li>
 * </ul>
 * Components of other types bind no meters, so the binder adapts to whatever was wired.
 *
 * <p>Trigger meters read the {@link ThresholdTrigger.Status} snapshot, which only advances
 * on the instance currently elected coordinator; on all other instances they keep their
 * initial values ({@code NaN}/0). Aggregate across instances with {@code max}, or join on
 * {@code consumer.balancer.coordinator == 1}.
 *
 * <p>The auto-configuration binds every {@code ConsumerGroupBalancers} bean. A registry the
 * application builds outside the context is bound with one call:
 * {@code ConsumerBalancerMetrics.of(balancers).bindTo(meterRegistry)}.
 */
public final class ConsumerBalancerMetrics implements MeterBinder {

    private final List<ConsumerGroupBalancers> balancers;
    // Spring Boot binds MeterBinder beans itself too; a second bind must not register a
    // second group listener on every registry.
    private final Set<MeterRegistry> boundTo = Collections.newSetFromMap(new IdentityHashMap<>());

    private ConsumerBalancerMetrics(List<ConsumerGroupBalancers> balancers) {
        this.balancers = List.copyOf(balancers);
    }

    public static ConsumerBalancerMetrics of(ConsumerGroupBalancers... balancers) {
        return new ConsumerBalancerMetrics(List.of(balancers));
    }

    public static ConsumerBalancerMetrics of(Collection<ConsumerGroupBalancers> balancers) {
        return new ConsumerBalancerMetrics(List.copyOf(balancers));
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        synchronized (boundTo) {
            if (!boundTo.add(registry)) {
                return;
            }
        }
        for (ConsumerGroupBalancers registryBalancers : balancers) {
            Tags tags = Tags.of(registryBalancers.getTags().entrySet().stream()
                    .map(tag -> Tag.of(tag.getKey(), tag.getValue()))
                    .toList());
            if (registryBalancers.getWeightService() instanceof KafkaOffsetRateWeightService offsetRate) {
                bindOffsetRateWeightService(registry, tags, offsetRate);
            }
            registryBalancers.onRegister(group -> bindGroup(registry, tags.and("group", group.getGroupId()), group));
        }
    }

    private static void bindGroup(MeterRegistry registry, Tags tags, ConsumerGroupBalancer group) {
        if (!group.isProactive()) {
            return;
        }
        bindCoordinator(registry, tags, group);
        if (group.getTrigger() instanceof ThresholdTrigger trigger) {
            bindTrigger(registry, tags, trigger);
        }
        if (group.getRebalanceInitiator() instanceof ContainerRebalanceInitiator initiator) {
            bindRebalanceInitiator(registry, tags, initiator);
        }
    }

    private static void bindTrigger(MeterRegistry registry, Tags tags, ThresholdTrigger trigger) {
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

    private static void bindCoordinator(MeterRegistry registry, Tags tags, ConsumerGroupBalancer group) {
        Gauge.builder("consumer.balancer.coordinator", group, g -> g.isCoordinator() ? 1.0 : 0.0)
                .description("1 while this instance holds the group's coordinator role, 0 otherwise")
                .tags(tags)
                .register(registry);
    }

    private static void bindRebalanceInitiator(
            MeterRegistry registry, Tags tags, ContainerRebalanceInitiator rebalanceInitiator) {
        FunctionCounter.builder("consumer.balancer.rebalance.initiations", rebalanceInitiator,
                        i -> i.getInitiations() - i.getNoMatchInitiations())
                .description("Proactive rebalance initiations by whether any listener container matched")
                .tags(tags)
                .tag("result", "enforced")
                .register(registry);
        FunctionCounter.builder("consumer.balancer.rebalance.initiations", rebalanceInitiator,
                        ContainerRebalanceInitiator::getNoMatchInitiations)
                .description("Proactive rebalance initiations by whether any listener container matched")
                .tags(tags)
                .tag("result", "no_match")
                .register(registry);
        FunctionCounter.builder("consumer.balancer.rebalance.containers.enforced", rebalanceInitiator,
                        ContainerRebalanceInitiator::getContainersEnforced)
                .description("Listener containers on which a rebalance was enforced")
                .tags(tags)
                .register(registry);
    }

    private static void bindOffsetRateWeightService(
            MeterRegistry registry, Tags tags, KafkaOffsetRateWeightService offsetRateWeightService) {
        FunctionCounter.builder("consumer.balancer.offset.rate.sample.errors", offsetRateWeightService,
                        KafkaOffsetRateWeightService::getSampleErrors)
                .description("Background end-offset samples in which at least one partition failed;"
                        + " persistent failures degrade weights toward the default")
                .tags(tags)
                .register(registry);
        Gauge.builder("consumer.balancer.offset.rate.tracked.partitions", offsetRateWeightService,
                        KafkaOffsetRateWeightService::getTrackedPartitionCount)
                .description("Partitions tracked by the background end-offset sampler")
                .tags(tags)
                .register(registry);
    }
}
