package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Objects;

/**
 * Forces a rebalance on the listener containers of one consumer group. Containers of
 * other groups in the same application are left untouched.
 */
public class ContainerRegistryRebalanceInitiator implements CoordinatorManager.RebalanceInitiator {

    private final KafkaListenerEndpointRegistry registry;
    private final String groupId;

    public ContainerRegistryRebalanceInitiator(KafkaListenerEndpointRegistry registry, String groupId) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
    }

    @Override
    public void initiateRebalance() {
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            if (groupId.equals(container.getGroupId())) {
                container.enforceRebalance();
            }
        }
    }
}
