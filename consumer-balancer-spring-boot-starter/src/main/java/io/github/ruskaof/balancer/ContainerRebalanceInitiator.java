package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.trigger.RebalanceInitiator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Forces a rebalance on the listener containers of one consumer group by calling
 * {@code enforceRebalance()} on them. Containers of other groups are left untouched.
 *
 * <p>The containers come from any source: a {@link ListenerContainerRegistry} such as the
 * {@code KafkaListenerEndpointRegistry} behind {@code @KafkaListener}, or — for containers an
 * application creates and replaces itself, e.g. whenever it rescans its topics — any
 * {@link Supplier} of the current containers. The supplier is called on every rebalance, so it
 * always sees the latest containers.
 *
 * <p><b>The group id does not identify a cluster.</b> An application consuming from several
 * Kafka clusters normally reuses the same group id on each of them, and every one of those
 * clusters has its own balancer. Selecting containers by group id alone would let the trigger
 * of one cluster rebalance the containers of all of them. Narrow the selection with
 * {@link #onlyListenerIds(Collection)} or {@link #filter(Predicate)}, so each balancer only
 * touches the containers that consume from its own cluster.
 */
@Slf4j
public class ContainerRebalanceInitiator implements RebalanceInitiator {

    private final String groupId;
    private final Supplier<? extends Collection<? extends MessageListenerContainer>> containers;
    private final Predicate<MessageListenerContainer> containerFilter;
    private final String filterDescription;

    // The coordinator's scheduler thread writes, metrics scrape threads read.
    private final AtomicLong initiations = new AtomicLong();
    private final AtomicLong noMatchInitiations = new AtomicLong();
    private final AtomicLong containersEnforced = new AtomicLong();

    private ContainerRebalanceInitiator(
            String groupId,
            Supplier<? extends Collection<? extends MessageListenerContainer>> containers,
            Predicate<MessageListenerContainer> containerFilter,
            String filterDescription) {
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.containers = Objects.requireNonNull(containers, "containers");
        this.containerFilter = Objects.requireNonNull(containerFilter, "containerFilter");
        this.filterDescription = filterDescription;
    }

    /**
     * Every container of {@code groupId} that {@code containers} returns at rebalance time.
     *
     * @param containers the application's current listener containers; called on every
     *                   rebalance, from the coordinator's scheduler thread
     */
    public static ContainerRebalanceInitiator of(
            String groupId,
            Supplier<? extends Collection<? extends MessageListenerContainer>> containers) {
        return new ContainerRebalanceInitiator(groupId, containers, container -> true, null);
    }

    /** Every container of {@code groupId} registered with {@code registry}, on every cluster. */
    public static ContainerRebalanceInitiator of(String groupId, ListenerContainerRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        return of(groupId, registry::getListenerContainers);
    }

    /**
     * Narrows the selection to the containers with these listener ids — the {@code id} of a
     * {@code @KafkaListener}, or the bean name of a programmatically registered endpoint.
     * Containers of a retry topic are matched by their main listener id too, so a listener and
     * its retry containers stay together.
     *
     * <p>This is the least surprising way to scope a balancer to one cluster in a
     * multi-cluster application: listener ids are the only stable, publicly readable identity
     * a {@link MessageListenerContainer} carries besides its group id.
     *
     * @return a new initiator; this one is left unchanged
     * @throws IllegalArgumentException when {@code listenerIds} is empty
     */
    public ContainerRebalanceInitiator onlyListenerIds(Collection<String> listenerIds) {
        Set<String> ids = new HashSet<>(Objects.requireNonNull(listenerIds, "listenerIds"));
        if (ids.isEmpty()) {
            throw new IllegalArgumentException("At least one listener id is required");
        }
        return narrowed(
                container -> ids.contains(container.getListenerId()) || ids.contains(container.getMainListenerId()),
                "listener ids " + new TreeSet<>(ids));
    }

    /**
     * Narrows the selection to the containers {@code containerFilter} accepts, on top of the
     * group id match and any earlier narrowing.
     *
     * @return a new initiator; this one is left unchanged
     */
    public ContainerRebalanceInitiator filter(Predicate<MessageListenerContainer> containerFilter) {
        return narrowed(Objects.requireNonNull(containerFilter, "containerFilter"), "a custom container filter");
    }

    private ContainerRebalanceInitiator narrowed(Predicate<MessageListenerContainer> filter, String description) {
        return new ContainerRebalanceInitiator(
                groupId,
                containers,
                containerFilter.and(filter),
                filterDescription == null ? description : filterDescription + " and " + description);
    }

    @Override
    public void initiateRebalance() {
        initiations.incrementAndGet();
        int rebalanced = 0;
        for (MessageListenerContainer container : containers.get()) {
            if (groupId.equals(container.getGroupId()) && containerFilter.test(container)) {
                container.enforceRebalance();
                rebalanced++;
            }
        }
        containersEnforced.addAndGet(rebalanced);
        String selection = filterDescription == null ? "any container of the group" : filterDescription;
        if (rebalanced == 0) {
            noMatchInitiations.incrementAndGet();
            // Silently doing nothing would leave the trigger firing forever against an
            // assignment it can never change.
            log.warn("No listener container matched group '{}' and {}; the proactive rebalance had no effect."
                            + " Check the group id, the container filter, and that the container source returns"
                            + " the group's current containers.",
                    groupId, selection);
        } else {
            log.info("Enforced a rebalance on {} listener container(s) of group '{}' ({})",
                    rebalanced, groupId, selection);
        }
    }

    public String getGroupId() {
        return groupId;
    }

    /** Times {@link #initiateRebalance()} was called; monotonic. */
    public long getInitiations() {
        return initiations.get();
    }

    /** Initiations on which no container matched; monotonic. */
    public long getNoMatchInitiations() {
        return noMatchInitiations.get();
    }

    /** Containers that received {@code enforceRebalance()} across all initiations; monotonic. */
    public long getContainersEnforced() {
        return containersEnforced.get();
    }
}
