package io.github.ruskaof.balancer.autoconfigure;

import org.junit.jupiter.api.Test;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * The proactive path is on by default, so a missing spring.kafka.consumer.group-id must
 * fail with a message naming the property and the switch that disables the path.
 */
class BalancerAutoConfigurationGroupIdTest {

    @Test
    void proactiveBeansFailWithActionableMessageWithoutConsumerGroupId() {
        var configuration = new BalancerAutoConfiguration.ProactiveRebalanceConfiguration();
        KafkaProperties kafkaProperties = new KafkaProperties();

        assertThatThrownBy(() -> configuration.rebalanceInitiator(
                mock(KafkaListenerEndpointRegistry.class), kafkaProperties, new KafkaBalancerProperties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("spring.kafka.consumer.group-id")
                .hasMessageContaining("consumer-balancer.proactive-rebalance-enabled=false");
    }
}
