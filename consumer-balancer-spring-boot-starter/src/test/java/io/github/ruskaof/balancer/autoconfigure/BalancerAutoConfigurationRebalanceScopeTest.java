package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.trigger.RebalanceDamping;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * consumer-balancer.listener-ids narrows the initiator of the auto-registered group to the
 * containers of one Kafka cluster, for applications that reuse a group id across clusters.
 */
class BalancerAutoConfigurationRebalanceScopeTest {

    private final KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);

    @Test
    void rebalancesEveryContainerOfTheGroupByDefault() {
        MessageListenerContainer orders = container("orders");
        MessageListenerContainer paymentsOnClusterB = container("payments-on-cluster-b");
        when(registry.getListenerContainers()).thenReturn(List.of(orders, paymentsOnClusterB));

        BalancerAutoConfiguration.defaultRebalanceInitiator("shared-group", registry, new KafkaBalancerProperties())
                .initiateRebalance();

        verify(orders).enforceRebalance();
        verify(paymentsOnClusterB).enforceRebalance();
    }

    @Test
    void rebalancesOnlyTheConfiguredListenersWhenListenerIdsAreSet() {
        MessageListenerContainer orders = container("orders");
        MessageListenerContainer paymentsOnClusterB = container("payments-on-cluster-b");
        when(registry.getListenerContainers()).thenReturn(List.of(orders, paymentsOnClusterB));
        KafkaBalancerProperties properties = new KafkaBalancerProperties();
        properties.setListenerIds(List.of("orders"));

        BalancerAutoConfiguration.defaultRebalanceInitiator("shared-group", registry, properties)
                .initiateRebalance();

        verify(orders).enforceRebalance();
        verify(paymentsOnClusterB, never()).enforceRebalance();
    }

    @Test
    void defaultsFavourReluctance() {
        KafkaBalancerProperties properties = new KafkaBalancerProperties();

        assertThat(properties.getListenerIds()).isEmpty();
        assertThat(properties.getRebalanceLoadImbalanceThreshold()).isEqualTo(1.1d);
        assertThat(properties.toRebalanceDamping()).isEqualTo(RebalanceDamping.defaults());
    }

    private static MessageListenerContainer container(String listenerId) {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.getGroupId()).thenReturn("shared-group");
        when(container.getListenerId()).thenReturn(listenerId);
        return container;
    }
}
