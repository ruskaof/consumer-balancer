package io.github.ruskaof.balancer;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class ContainerRebalanceInitiatorTest {

    private final KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);

    @Test
    void enforcesRebalanceOnlyOnContainersOfTheCoordinatedGroup() {
        MessageListenerContainer coordinatedGroupContainer = container("coordinated-group", "orders");
        MessageListenerContainer otherGroupContainer = container("other-group", "payments");
        register(coordinatedGroupContainer, otherGroupContainer);

        ContainerRebalanceInitiator.of("coordinated-group", registry).initiateRebalance();

        verify(coordinatedGroupContainer).enforceRebalance();
        verify(otherGroupContainer, never()).enforceRebalance();
    }

    @Test
    void readsSelfManagedContainersFreshOnEveryRebalance() {
        // Containers an application replaces whenever it rescans its topics, never registered
        // with any KafkaListenerEndpointRegistry.
        List<MessageListenerContainer> managed = new ArrayList<>();
        ContainerRebalanceInitiator initiator = ContainerRebalanceInitiator.of("orders-group", () -> managed);
        MessageListenerContainer first = container("orders-group", null);
        managed.add(first);
        initiator.initiateRebalance();

        MessageListenerContainer replacement = container("orders-group", null);
        managed.set(0, replacement);
        initiator.initiateRebalance();

        verify(first, times(1)).enforceRebalance();
        verify(replacement, times(1)).enforceRebalance();
        assertThat(initiator.getContainersEnforced()).isEqualTo(2);
    }

    @Test
    void enforcesRebalanceOnlyOnTheListenersOfThisCluster() {
        // Both clusters are consumed under one group id, so only the listener ids tell the
        // containers of this balancer's cluster from the other cluster's.
        MessageListenerContainer thisCluster = container("shared-group", "orders");
        MessageListenerContainer otherCluster = container("shared-group", "orders-on-cluster-b");
        register(thisCluster, otherCluster);

        ContainerRebalanceInitiator.of("shared-group", registry)
                .onlyListenerIds(List.of("orders"))
                .initiateRebalance();

        verify(thisCluster).enforceRebalance();
        verify(otherCluster, never()).enforceRebalance();
    }

    @Test
    void enforcesRebalanceOnTheRetryContainersOfASelectedListener() {
        MessageListenerContainer retryContainer = mock(MessageListenerContainer.class);
        when(retryContainer.getGroupId()).thenReturn("shared-group");
        when(retryContainer.getListenerId()).thenReturn("orders-retry-0");
        when(retryContainer.getMainListenerId()).thenReturn("orders");
        register(retryContainer);

        ContainerRebalanceInitiator.of("shared-group", registry)
                .onlyListenerIds(List.of("orders"))
                .initiateRebalance();

        verify(retryContainer).enforceRebalance();
    }

    @Test
    void appliesAnArbitraryContainerFilterOnTopOfTheGroupId() {
        MessageListenerContainer selected = container("shared-group", "orders");
        MessageListenerContainer rejected = container("shared-group", "payments");
        register(selected, rejected);

        ContainerRebalanceInitiator.of("shared-group", registry)
                .filter(selected::equals)
                .initiateRebalance();

        verify(selected).enforceRebalance();
        verify(rejected, never()).enforceRebalance();
    }

    @Test
    void narrowingCombinesAndLeavesTheOriginalUntouched() {
        MessageListenerContainer orders = container("shared-group", "orders");
        MessageListenerContainer payments = container("shared-group", "payments");
        register(orders, payments);
        ContainerRebalanceInitiator everything = ContainerRebalanceInitiator.of("shared-group", registry);

        everything.onlyListenerIds(List.of("orders", "payments"))
                .filter(container -> !container.equals(payments))
                .initiateRebalance();

        verify(orders).enforceRebalance();
        verify(payments, never()).enforceRebalance();
        assertThat(everything.getInitiations()).isZero();
    }

    @Test
    void doesNothingWhenNoContainerMatches() {
        MessageListenerContainer otherGroupContainer = container("other-group", "payments");
        register(otherGroupContainer);

        ContainerRebalanceInitiator.of("coordinated-group", registry).initiateRebalance();

        verify(otherGroupContainer, never()).enforceRebalance();
    }

    @Test
    void countsInitiationsAndEnforcedContainers() {
        register(container("coordinated-group", "orders"));
        ContainerRebalanceInitiator initiator = ContainerRebalanceInitiator.of("coordinated-group", registry);

        initiator.initiateRebalance();

        assertThat(initiator.getInitiations()).isEqualTo(1);
        assertThat(initiator.getContainersEnforced()).isEqualTo(1);
        assertThat(initiator.getNoMatchInitiations()).isZero();
    }

    @Test
    void countsInitiationsThatMatchedNoContainer() {
        register(container("other-group", "payments"));
        ContainerRebalanceInitiator initiator = ContainerRebalanceInitiator.of("coordinated-group", registry);

        initiator.initiateRebalance();

        assertThat(initiator.getInitiations()).isEqualTo(1);
        assertThat(initiator.getContainersEnforced()).isZero();
        assertThat(initiator.getNoMatchInitiations()).isEqualTo(1);
    }

    @Test
    void rejectsAnEmptyListenerIdSelection() {
        assertThatThrownBy(() -> ContainerRebalanceInitiator.of("shared-group", registry).onlyListenerIds(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("listener id");
    }

    private void register(MessageListenerContainer... containers) {
        when(registry.getListenerContainers()).thenReturn(List.of(containers));
    }

    private static MessageListenerContainer container(String groupId, String listenerId) {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.getGroupId()).thenReturn(groupId);
        when(container.getListenerId()).thenReturn(listenerId);
        return container;
    }
}
