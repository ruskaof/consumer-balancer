package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.prometheus.TemplatedKafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import io.github.ruskaof.balancer.prometheus.PrometheusConnectionSettings;
import io.github.ruskaof.balancer.prometheus.PrometheusObjectMappers;
import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import io.github.ruskaof.balancer.weight.PrometheusWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Partition assignor that weights partitions by observed load and spreads them greedily.
 *
 * <p>Collaborators are taken from the consumer configs (see {@link LoadAwareAssignorConfig}):
 * {@code assignor.load-aware.weight-service}, {@code assignor.load-aware.balance-service} and
 * {@code assignor.load-aware.member-id-tracker} each accept an instance, a {@link Class} or a
 * class name. When no weight service is configured, a Prometheus-backed default is built from
 * the {@code assignor.load-aware.prometheus.*} configs.
 */
@NoArgsConstructor // For Kafka's reflection-based instantiation
public class LoadAwarePartitionAssignor extends AbstractPartitionAssignor implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(LoadAwarePartitionAssignor.class);

    private WeightService weightService = null;
    private BalanceService balanceService = null;
    private MemberIdTracker memberIdTracker = null;
    private String lastReportedMemberId = null;
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
            Map<String, Subscription> subscriptions) {

        Set<TopicPartition> allPartitions = getAllPartitions(partitionsPerTopic);
        Map<TopicPartition, Double> weights = sanitizedWeights(
                allPartitions, weightService.computeWeights(allPartitions));

        Map<String, Set<String>> subscribedTopicsByMember = new HashMap<>();
        subscriptions.forEach((member, subscription) ->
                subscribedTopicsByMember.put(member, Set.copyOf(subscription.topics())));

        return balanceService.computeOptimalAssignment(subscribedTopicsByMember, weights);
    }

    /**
     * Restricts weights to the partitions being assigned so the assignment covers exactly
     * {@code allPartitions}: entries the weight service did not return (or returned as
     * {@code null}/non-finite) fall back to {@link PartitionWeightDefaults#MISSING}, and
     * entries for other partitions (e.g. stale Prometheus series) are dropped.
     */
    private static Map<TopicPartition, Double> sanitizedWeights(
            Set<TopicPartition> allPartitions,
            Map<TopicPartition, Double> rawWeights) {
        Map<TopicPartition, Double> weights = new HashMap<>();
        int defaulted = 0;
        for (TopicPartition tp : allPartitions) {
            Double weight = rawWeights == null ? null : rawWeights.get(tp);
            if (weight == null || !Double.isFinite(weight)) {
                weight = PartitionWeightDefaults.MISSING;
                defaulted++;
            }
            weights.put(tp, weight);
        }
        if (defaulted > 0) {
            log.warn("{} of {} partitions had no usable weight; using default weight {}",
                    defaulted, allPartitions.size(), PartitionWeightDefaults.MISSING);
        }
        return weights;
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
        BalanceService configuredBalanceService = ConfigInstanceResolver.resolveOrNull(
                configs, LoadAwareAssignorConfig.BALANCE_SERVICE, BalanceService.class);
        this.balanceService = configuredBalanceService != null
                ? configuredBalanceService
                : new SortingRoundRobinBalanceService();

        WeightService configuredWeightService = ConfigInstanceResolver.resolveOrNull(
                configs, LoadAwareAssignorConfig.WEIGHT_SERVICE, WeightService.class);
        this.weightService = configuredWeightService != null
                ? configuredWeightService
                : createDefaultPrometheusWeightService(configs);

        this.memberIdTracker = ConfigInstanceResolver.resolveOrNull(
                configs, LoadAwareAssignorConfig.MEMBER_ID_TRACKER, MemberIdTracker.class);
    }

    /**
     * Reports this consumer's member id to the configured {@link MemberIdTracker} so
     * coordinator election can recognize member ids owned by this JVM.
     */
    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        if (memberIdTracker == null || metadata == null) {
            return;
        }
        String memberId = metadata.memberId();
        if (memberId == null || memberId.isBlank()) {
            return;
        }
        memberIdTracker.updateMemberId(metadata.groupId(), lastReportedMemberId, memberId);
        lastReportedMemberId = memberId;
    }

    private static PrometheusWeightService createDefaultPrometheusWeightService(Map<String, ?> configs) {
        PrometheusConnectionSettings settings = LoadAwareAssignorConfig.connectionSettingsFrom(configs);
        var prometheusClient = new PrometheusClient(settings, PrometheusObjectMappers.create());
        String weightQueryTemplate = requireNonBlank(
                configs,
                LoadAwareAssignorConfig.PROMETHEUS_WEIGHT_QUERY_TEMPLATE,
                "Required when using load-aware assignor: assignor.load-aware.prometheus.weight-query-template "
                        + "(must contain %s; not required when assignor.load-aware.weight-service is set)");
        return new PrometheusWeightService(
                new TemplatedKafkaRatePromqlBuilder(weightQueryTemplate),
                prometheusClient,
                LoadAwareAssignorConfig.stringConfig(
                        configs,
                        LoadAwareAssignorConfig.PROMETHEUS_TOPIC_LABEL,
                        PrometheusWeightService.DEFAULT_TOPIC_LABEL),
                LoadAwareAssignorConfig.stringConfig(
                        configs,
                        LoadAwareAssignorConfig.PROMETHEUS_PARTITION_LABEL,
                        PrometheusWeightService.DEFAULT_PARTITION_LABEL));
    }

    public static class LoadAwareAssignorConfig {
        /**
         * {@link WeightService} used by the assignor. Value: an instance, a {@link Class},
         * or a class name with a public no-arg constructor (a {@code Configurable}
         * implementation gets {@code configure(configs)} called). When absent, the
         * Prometheus-backed default applies and the {@code assignor.load-aware.prometheus.*}
         * configs become required.
         */
        public static final String WEIGHT_SERVICE = "assignor.load-aware.weight-service";
        /**
         * {@link BalanceService} used by the assignor. Value: an instance, a {@link Class},
         * or a class name. Default: {@link SortingRoundRobinBalanceService}.
         */
        public static final String BALANCE_SERVICE = "assignor.load-aware.balance-service";
        /**
         * Optional {@link MemberIdTracker} that receives this consumer's member id after
         * each rebalance. Value: an instance, a {@link Class}, or a class name. Pass the
         * same instance to {@code CoordinatorElection} for proactive rebalancing.
         */
        public static final String MEMBER_ID_TRACKER = "assignor.load-aware.member-id-tracker";

        public static final String PROMETHEUS_HOST = "assignor.load-aware.prometheus.host";
        public static final String PROMETHEUS_PORT = "assignor.load-aware.prometheus.port";
        public static final String PROMETHEUS_SCHEME = "assignor.load-aware.prometheus.scheme";
        /**
         * Optional path prefix prepended to {@code /api/v1/query} for
         * Prometheus-API-compatible backends, e.g. {@code /prometheus} for single-node
         * VictoriaMetrics or {@code /select/<accountID>/prometheus} for a
         * VictoriaMetrics cluster. Default: empty (plain Prometheus).
         */
        public static final String PROMETHEUS_PATH_PREFIX = "assignor.load-aware.prometheus.path-prefix";
        /**
         * Optional value for the {@code Authorization} header sent with every query,
         * e.g. {@code Bearer <token>} or {@code Basic <base64>}. Default: no header.
         */
        public static final String PROMETHEUS_AUTHORIZATION = "assignor.load-aware.prometheus.authorization";
        public static final String PROMETHEUS_CONNECT_TIMEOUT_MS = "assignor.load-aware.prometheus.connect-timeout-ms";
        public static final String PROMETHEUS_REQUEST_TIMEOUT_MS = "assignor.load-aware.prometheus.request-timeout-ms";
        /**
         * PromQL template with placeholder {@code %s} for topic regex (required).
         */
        public static final String PROMETHEUS_WEIGHT_QUERY_TEMPLATE = "assignor.load-aware.prometheus.weight-query-template";
        /**
         * Label on the weight-query series that carries the topic name.
         * Default: {@code topic}.
         */
        public static final String PROMETHEUS_TOPIC_LABEL = "assignor.load-aware.prometheus.topic-label";
        /**
         * Label on the weight-query series that carries the partition number.
         * Default: {@code partition}.
         */
        public static final String PROMETHEUS_PARTITION_LABEL = "assignor.load-aware.prometheus.partition-label";

        public static PrometheusConnectionSettings connectionSettingsFrom(Map<String, ?> configs) {
            Object hostObj = configs.get(PROMETHEUS_HOST);
            if (hostObj == null || hostObj.toString().isBlank()) {
                throw new IllegalArgumentException(
                        "Required when using load-aware assignor: assignor.load-aware.prometheus.host"
                                + " (not required when assignor.load-aware.weight-service is set)");
            }
            String host = hostObj.toString();

            Object portObj = configs.get(PROMETHEUS_PORT);
            if (portObj == null || portObj.toString().isBlank()) {
                throw new IllegalArgumentException(
                        "Required when using load-aware assignor: assignor.load-aware.prometheus.port"
                                + " (not required when assignor.load-aware.weight-service is set)");
            }
            int port;
            try {
                port = Integer.parseInt(portObj.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid port value for load-aware assignor: " + portObj, e);
            }
            String scheme = stringConfig(configs, PROMETHEUS_SCHEME, "http");
            String pathPrefix = stringConfig(configs, PROMETHEUS_PATH_PREFIX, "");
            String authorization = stringConfig(configs, PROMETHEUS_AUTHORIZATION, null);
            long connectMs = parseLong(configs, PROMETHEUS_CONNECT_TIMEOUT_MS, 10_000L);
            long requestMs = parseLong(configs, PROMETHEUS_REQUEST_TIMEOUT_MS, 30_000L);
            return new PrometheusConnectionSettings(
                    scheme,
                    host,
                    port,
                    pathPrefix,
                    authorization,
                    Duration.ofMillis(connectMs),
                    Duration.ofMillis(requestMs));
        }

        private static String stringConfig(Map<String, ?> configs, String key, String defaultValue) {
            Object value = configs.get(key);
            return value == null ? defaultValue : value.toString();
        }

        private static long parseLong(Map<String, ?> configs, String key, long defaultValue) {
            Object value = configs.get(key);
            if (value == null) {
                return defaultValue;
            }
            try {
                return Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid value for " + key + ": " + value, e);
            }
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
