package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.trigger.RebalanceDamping;
import org.junit.jupiter.api.Test;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * consumer-balancer.listener-ids narrows the auto-configured initiator to the containers of one
 * Kafka cluster, for applications that reuse a group id across clusters.
 */
class BalancerAutoConfigurationRebalanceScopeTest {

    private final BalancerAutoConfiguration.ProactiveRebalanceConfiguration configuration =
            new BalancerAutoConfiguration.ProactiveRebalanceConfiguration();
    private final KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);

    @Test
    void rebalancesEveryContainerOfTheGroupByDefault() {
        MessageListenerContainer orders = container("orders");
        MessageListenerContainer paymentsOnClusterB = container("payments-on-cluster-b");
        when(registry.getListenerContainers()).thenReturn(List.of(orders, paymentsOnClusterB));

        configuration.rebalanceInitiator(registry, kafkaProperties(), new KafkaBalancerProperties())
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

        configuration.rebalanceInitiator(registry, kafkaProperties(), properties).initiateRebalance();

        verify(orders).enforceRebalance();
        verify(paymentsOnClusterB, never()).enforceRebalance();
    }

    @Test
    void defaultsFavourReluctance() {
        KafkaBalancerProperties properties = new KafkaBalancerProperties();

        assertThat(properties.getListenerIds()).isEmpty();
        assertThat(properties.getRebalanceLoadImbalanceThreshold()).isEqualTo(1.2d);
        assertThat(properties.toRebalanceDamping()).isEqualTo(RebalanceDamping.defaults());
    }

    private static KafkaProperties kafkaProperties() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.getConsumer().setGroupId("shared-group");
        return kafkaProperties;
    }

    private static MessageListenerContainer container(String listenerId) {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.getGroupId()).thenReturn("shared-group");
        when(container.getListenerId()).thenReturn(listenerId);
        return container;
    }
}
