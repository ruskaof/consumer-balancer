package com.ruskaof.listener;

import com.ruskaof.listener.balance.BalanceService;
import com.ruskaof.listener.balance.SortingRoundRobinBalanceService;
import com.ruskaof.listener.prometheus.TemplatedKafkaRatePromqlBuilder;
import com.ruskaof.listener.prometheus.PrometheusClient;
import com.ruskaof.listener.prometheus.PrometheusConnectionSettings;
import com.ruskaof.listener.prometheus.PrometheusObjectMappers;
import com.ruskaof.listener.weight.PrometheusWeightService;
import com.ruskaof.listener.weight.WeightService;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

@NoArgsConstructor // For Kafka's reflection-based instantiation
public class LoadAwarePartitionAssignor extends AbstractPartitionAssignor implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(LoadAwarePartitionAssignor.class);

    private WeightService weightService = null;
    private BalanceService balanceService = null;
    private final RoundRobinAssignor fallbackAssignor = new RoundRobinAssignor();

    @Override
    public String name() {
        return "load-aware";
    }

    @Override
    public Map<String, List<TopicPartition>> assign(
            Map<String, Integer> partitionsPerTopic,
            Map<String, Subscription> subscriptions) {

        try {
            return assignWithLoadAwareness(partitionsPerTopic, subscriptions);
        } catch (Exception e) {
            log.warn("Load-aware assignment failed. Falling back to round-robin assignment.", e);
            return fallbackAssignor.assign(partitionsPerTopic, subscriptions);
        }
    }
    
    private Map<String, List<TopicPartition>> assignWithLoadAwareness(
            Map<String, Integer> partitionsPerTopic,
            Map<String, Subscription> subscriptions)  {

        var weights = weightService.computeWeights(getAllPartitions(partitionsPerTopic));
        var members = subscriptions.keySet();

        return balanceService.computeOptimalAssignment(members, weights);
    }

    private static Set<TopicPartition> getAllPartitions(Map<String, Integer> partitionsPerTopic) {
        Set<TopicPartition> allPartitions = new HashSet<>();
        for (Map.Entry<String, Integer> entry : partitionsPerTopic.entrySet()) {
            String topic = entry.getKey();
            int numPartitions = entry.getValue();
            for (int i = 0; i < numPartitions; i++) {
                allPartitions.add(new TopicPartition(topic, i));
            }
        }
        return allPartitions;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        PrometheusConnectionSettings settings = LoadAwareAssignorConfig.connectionSettingsFrom(configs);
        var prometheusClient = new PrometheusClient(settings, PrometheusObjectMappers.create());

        this.balanceService = new SortingRoundRobinBalanceService();
        String weightQueryTemplate = requireNonBlank(
                configs,
                LoadAwareAssignorConfig.PROMETHEUS_WEIGHT_QUERY_TEMPLATE,
                "Required when using load-aware assignor: assignor.load-aware.prometheus.weight-query-template "
                        + "(must contain %s)"
        );
        this.weightService = new PrometheusWeightService(
                new TemplatedKafkaRatePromqlBuilder(weightQueryTemplate),
                prometheusClient
        );
    }

    public static class LoadAwareAssignorConfig {
        public static final String PROMETHEUS_HOST = "assignor.load-aware.prometheus.host";
        public static final String PROMETHEUS_PORT = "assignor.load-aware.prometheus.port";
        public static final String PROMETHEUS_SCHEME = "assignor.load-aware.prometheus.scheme";
        public static final String PROMETHEUS_CONNECT_TIMEOUT_MS = "assignor.load-aware.prometheus.connect-timeout-ms";
        public static final String PROMETHEUS_REQUEST_TIMEOUT_MS = "assignor.load-aware.prometheus.request-timeout-ms";
        /**
         * PromQL template with placeholder {@code %s} for topic regex (required).
         */
        public static final String PROMETHEUS_WEIGHT_QUERY_TEMPLATE = "assignor.load-aware.prometheus.weight-query-template";

        public static PrometheusConnectionSettings connectionSettingsFrom(Map<String, ?> configs) {
            String host = configs.get(PROMETHEUS_HOST).toString();
            int port = Integer.parseInt(configs.get(PROMETHEUS_PORT).toString());
            String scheme = configs.containsKey(PROMETHEUS_SCHEME)
                    ? configs.get(PROMETHEUS_SCHEME).toString()
                    : "http";
            long connectMs = parseLong(configs, PROMETHEUS_CONNECT_TIMEOUT_MS, 10_000L);
            long requestMs = parseLong(configs, PROMETHEUS_REQUEST_TIMEOUT_MS, 30_000L);
            return new PrometheusConnectionSettings(
                    scheme,
                    host,
                    port,
                    Duration.ofMillis(connectMs),
                    Duration.ofMillis(requestMs)
            );
        }

        private static long parseLong(Map<String, ?> configs, String key, long defaultValue) {
            if (!configs.containsKey(key) || configs.get(key) == null) {
                return defaultValue;
            }
            return Long.parseLong(configs.get(key).toString());
        }
    }

    private static String requireNonBlank(Map<String, ?> configs, String key, String message) {
        Object v = configs.get(key);
        if (v == null || v.toString().isBlank()) {
            throw new IllegalArgumentException(message);
        }
        return v.toString();
    }
}
