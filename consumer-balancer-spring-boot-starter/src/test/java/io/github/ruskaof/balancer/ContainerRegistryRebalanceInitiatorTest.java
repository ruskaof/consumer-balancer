package io.github.ruskaof.balancer;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class ContainerRegistryRebalanceInitiatorTest {

    private final KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);

    @Test
    void enforcesRebalanceOnlyOnContainersOfTheCoordinatedGroup() {
        MessageListenerContainer coordinatedGroupContainer = container("coordinated-group", "orders");
        MessageListenerContainer otherGroupContainer = container("other-group", "payments");
        register(coordinatedGroupContainer, otherGroupContainer);

        new ContainerRegistryRebalanceInitiator(registry, "coordinated-group").initiateRebalance();

        verify(coordinatedGroupContainer).enforceRebalance();
        verify(otherGroupContainer, never()).enforceRebalance();
    }

    @Test
    void enforcesRebalanceOnlyOnTheListenersOfThisCluster() {
        // Both clusters are consumed under one group id, so only the listener ids tell the
        // containers of this balancer's cluster from the other cluster's.
        MessageListenerContainer thisCluster = container("shared-group", "orders");
        MessageListenerContainer otherCluster = container("shared-group", "orders-on-cluster-b");
        register(thisCluster, otherCluster);

        ContainerRegistryRebalanceInitiator.withListenerIds(registry, "shared-group", List.of("orders"))
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

        ContainerRegistryRebalanceInitiator.withListenerIds(registry, "shared-group", List.of("orders"))
                .initiateRebalance();

        verify(retryContainer).enforceRebalance();
    }

    @Test
    void appliesAnArbitraryContainerFilterOnTopOfTheGroupId() {
        MessageListenerContainer selected = container("shared-group", "orders");
        MessageListenerContainer rejected = container("shared-group", "payments");
        register(selected, rejected);

        new ContainerRegistryRebalanceInitiator(registry, "shared-group", selected::equals)
                .initiateRebalance();

        verify(selected).enforceRebalance();
        verify(rejected, never()).enforceRebalance();
    }

    @Test
    void doesNothingWhenNoContainerMatches() {
        MessageListenerContainer otherGroupContainer = container("other-group", "payments");
        register(otherGroupContainer);

        new ContainerRegistryRebalanceInitiator(registry, "coordinated-group").initiateRebalance();

        verify(otherGroupContainer, never()).enforceRebalance();
    }

    @Test
    void rejectsAnEmptyListenerIdSelection() {
        assertThatThrownBy(() ->
                ContainerRegistryRebalanceInitiator.withListenerIds(registry, "shared-group", List.of()))
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
