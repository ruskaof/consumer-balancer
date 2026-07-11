package io.github.ruskaof.balancer;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static org.mockito.Mockito.*;

class ContainerRegistryRebalanceInitiatorTest {

    @Test
    void enforcesRebalanceOnlyOnContainersOfTheCoordinatedGroup() {
        MessageListenerContainer coordinatedGroupContainer = mock(MessageListenerContainer.class);
        when(coordinatedGroupContainer.getGroupId()).thenReturn("coordinated-group");
        MessageListenerContainer otherGroupContainer = mock(MessageListenerContainer.class);
        when(otherGroupContainer.getGroupId()).thenReturn("other-group");

        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getListenerContainers())
                .thenReturn(List.of(coordinatedGroupContainer, otherGroupContainer));

        new ContainerRegistryRebalanceInitiator(registry, "coordinated-group").initiateRebalance();

        verify(coordinatedGroupContainer).enforceRebalance();
        verify(otherGroupContainer, never()).enforceRebalance();
    }
}
