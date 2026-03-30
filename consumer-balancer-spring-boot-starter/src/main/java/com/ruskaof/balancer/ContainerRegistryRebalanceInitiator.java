package com.ruskaof.balancer;

import com.ruskaof.balancer.trigger.CoordinatorManager;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

public class ContainerRegistryRebalanceInitiator implements CoordinatorManager.RebalanceInitiator {

    private final KafkaListenerEndpointRegistry registry;

    public ContainerRegistryRebalanceInitiator(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void initiateRebalance() {
        registry.getListenerContainers().forEach((MessageListenerContainer::enforceRebalance));
    }
}
