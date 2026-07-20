package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Injects the application context's {@link WeightService} and {@link BalanceService}
 * (and, when proactive rebalance is enabled, the {@link MemberIdTracker}), plus the
 * configured instance id, into Boot's auto-configured consumer factory, so
 * {@link LoadAwarePartitionAssignor} uses the same collaborators as the rebalance trigger.
 *
 * <p>Explicit user-provided values under the same keys (e.g. from
 * {@code spring.kafka.consumer.properties.*}) win over the context beans.
 */
@RequiredArgsConstructor
public class BalancerConsumerFactoryCustomizer implements DefaultKafkaConsumerFactoryCustomizer {

    private final WeightService weightService;
    private final BalanceService balanceService;
    private final MemberIdTracker memberIdTracker; // null when proactive rebalance is disabled
    private final String instanceId; // null lets the assignor use its per-JVM random id

    @Override
    public void customize(DefaultKafkaConsumerFactory<?, ?> consumerFactory) {
        Map<String, Object> existing = consumerFactory.getConfigurationProperties();
        Map<String, Object> updates = new HashMap<>();
        putIfAbsent(existing, updates, LoadAwareAssignorConfig.WEIGHT_SERVICE, weightService);
        putIfAbsent(existing, updates, LoadAwareAssignorConfig.BALANCE_SERVICE, balanceService);
        putIfAbsent(existing, updates, LoadAwareAssignorConfig.MEMBER_ID_TRACKER, memberIdTracker);
        if (instanceId != null && !instanceId.isBlank()) {
            putIfAbsent(existing, updates, LoadAwareAssignorConfig.INSTANCE_ID, instanceId);
        }
        if (!updates.isEmpty()) {
            consumerFactory.updateConfigs(updates);
        }
    }

    private static void putIfAbsent(Map<String, Object> existing, Map<String, Object> updates,
            String key, Object value) {
        if (value != null && !existing.containsKey(key)) {
            updates.put(key, value);
        }
    }
}
