package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Forces a rebalance on the listener containers of one consumer group. Containers of other
 * groups in the same application are left untouched.
 *
 * <p><b>The group id does not identify a cluster.</b> An application consuming from several
 * Kafka clusters normally reuses the same group id on each of them, and every one of those
 * clusters has its own balancer stack — its own admin client, weight store, coordinator
 * election and trigger. Selecting containers by group id alone would let the trigger of one
 * cluster rebalance the containers of all of them. Narrow the selection with
 * {@link #withListenerIds(KafkaListenerEndpointRegistry, String, Collection)} or an arbitrary
 * {@code containerFilter}, so each stack only touches the containers that consume from its own
 * cluster.
 */
@Slf4j
public class ContainerRegistryRebalanceInitiator implements CoordinatorManager.RebalanceInitiator {

    private final KafkaListenerEndpointRegistry registry;
    private final String groupId;
    private final Predicate<MessageListenerContainer> containerFilter;
    private final String filterDescription;

    // The coordinator's scheduler thread writes, metrics scrape threads read.
    private final AtomicLong initiations = new AtomicLong();
    private final AtomicLong noMatchInitiations = new AtomicLong();
    private final AtomicLong containersEnforced = new AtomicLong();

    /** Every registered container of {@code groupId}, on every cluster. */
    public ContainerRegistryRebalanceInitiator(KafkaListenerEndpointRegistry registry, String groupId) {
        this(registry, groupId, container -> true, "any container of the group");
    }

    /**
     * @param containerFilter applied on top of the group id match; only containers it accepts
     *                        are rebalanced
     */
    public ContainerRegistryRebalanceInitiator(
            KafkaListenerEndpointRegistry registry,
            String groupId,
            Predicate<MessageListenerContainer> containerFilter) {
        this(registry, groupId, containerFilter, "a custom container filter");
    }

    private ContainerRegistryRebalanceInitiator(
            KafkaListenerEndpointRegistry registry,
            String groupId,
            Predicate<MessageListenerContainer> containerFilter,
            String filterDescription) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.containerFilter = Objects.requireNonNull(containerFilter, "containerFilter");
        this.filterDescription = filterDescription;
    }

    /**
     * Restricts the rebalance to the containers with these listener ids — the {@code id} of a
     * {@code @KafkaListener}, or the bean name of a programmatically registered endpoint.
     * Containers of a retry topic are matched by their main listener id too, so a listener and
     * its retry containers stay together.
     *
     * <p>This is the least surprising way to scope a balancer to one cluster in a
     * multi-cluster application: listener ids are the only stable, publicly readable identity
     * a {@link MessageListenerContainer} carries besides its group id.
     *
     * @throws IllegalArgumentException when {@code listenerIds} is empty — use the
     *                                  {@linkplain #ContainerRegistryRebalanceInitiator(KafkaListenerEndpointRegistry, String)
     *                                  group-only constructor} to select every container instead
     */
    public static ContainerRegistryRebalanceInitiator withListenerIds(
            KafkaListenerEndpointRegistry registry,
            String groupId,
            Collection<String> listenerIds) {
        Set<String> ids = new HashSet<>(Objects.requireNonNull(listenerIds, "listenerIds"));
        if (ids.isEmpty()) {
            throw new IllegalArgumentException("At least one listener id is required");
        }
        return new ContainerRegistryRebalanceInitiator(
                registry,
                groupId,
                container -> ids.contains(container.getListenerId())
                        || ids.contains(container.getMainListenerId()),
                "listener ids " + new TreeSet<>(ids));
    }

    @Override
    public void initiateRebalance() {
        initiations.incrementAndGet();
        int rebalanced = 0;
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            if (groupId.equals(container.getGroupId()) && containerFilter.test(container)) {
                container.enforceRebalance();
                rebalanced++;
            }
        }
        containersEnforced.addAndGet(rebalanced);
        if (rebalanced == 0) {
            noMatchInitiations.incrementAndGet();
            // Silently doing nothing would leave the trigger firing forever against an
            // assignment it can never change.
            log.warn("No registered listener container matched group '{}' and {}; the proactive rebalance had"
                            + " no effect. Check the group id, the container filter, and that the containers are"
                            + " registered with this KafkaListenerEndpointRegistry.",
                    groupId, filterDescription);
        } else {
            log.info("Enforced a rebalance on {} listener container(s) of group '{}' ({})",
                    rebalanced, groupId, filterDescription);
        }
    }

    /** Times {@link #initiateRebalance()} was called; monotonic. */
    public long getInitiations() {
        return initiations.get();
    }

    /** Initiations on which no registered container matched; monotonic. */
    public long getNoMatchInitiations() {
        return noMatchInitiations.get();
    }

    /** Containers that received {@code enforceRebalance()} across all initiations; monotonic. */
    public long getContainersEnforced() {
        return containersEnforced.get();
    }
}
