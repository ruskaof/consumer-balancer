package io.github.ruskaof.balancer;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Puts a {@link ConsumerGroupBalancers} registry's {@linkplain ConsumerGroupBalancers#assignorConfigs()
 * assignor configs} into a consumer factory, so {@link LoadAwarePartitionAssignor} uses the same
 * collaborators as the registry's rebalance triggers. The auto-configured instance customizes
 * Boot's auto-configured consumer factory; it can be applied to any other
 * {@link DefaultKafkaConsumerFactory} by hand.
 *
 * <p>Explicit user-provided values under the same keys (e.g. from
 * {@code spring.kafka.consumer.properties.*}) win over the registry's.
 */
@RequiredArgsConstructor
public class BalancerConsumerFactoryCustomizer implements DefaultKafkaConsumerFactoryCustomizer {

    private final ConsumerGroupBalancers balancers;

    @Override
    public void customize(DefaultKafkaConsumerFactory<?, ?> consumerFactory) {
        Map<String, Object> existing = consumerFactory.getConfigurationProperties();
        Map<String, Object> updates = new HashMap<>();
        balancers.assignorConfigs().forEach((key, value) -> {
            if (!existing.containsKey(key)) {
                updates.put(key, value);
            }
        });
        if (!updates.isEmpty()) {
            consumerFactory.updateConfigs(updates);
        }
    }
}
