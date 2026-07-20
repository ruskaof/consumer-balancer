package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.instance.InstanceIdResolver;
import io.github.ruskaof.balancer.instance.InstanceUserData;
import io.github.ruskaof.balancer.prometheus.TemplatedKafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import io.github.ruskaof.balancer.prometheus.PrometheusConnectionSettings;
import io.github.ruskaof.balancer.prometheus.PrometheusObjectMappers;
import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.github.ruskaof.balancer.weight.PartitionWeights;
import io.github.ruskaof.balancer.weight.PrometheusWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

/**
 * Partition assignor that weights partitions by observed load and spreads them greedily,
 * evening traffic across application instances (pods/JVMs) first and across the members of
 * each instance second. Every member reports its instance id — configured or auto-resolved
 * by {@link InstanceIdResolver} — to the group leader through subscription userData, so the
 * leader can group co-located members.
 *
 * <p>Collaborators are taken from the consumer configs (see {@link LoadAwareAssignorConfig}):
 * {@code assignor.load-aware.weight-service}, {@code assignor.load-aware.balance-service} and
 * {@code assignor.load-aware.member-id-tracker} each accept an instance, a {@link Class} or a
 * class name. When no weight service is configured, a default is built according to
 * {@code assignor.load-aware.weight-store}: the offset-rate store (default) measures
 * per-partition events/sec through an admin client built from the consumer's own
 * {@code bootstrap.servers}/security configs, while {@code prometheus} builds a
 * Prometheus-backed store from the {@code assignor.load-aware.prometheus.*} configs.
 */
@NoArgsConstructor // For Kafka's reflection-based instantiation
public class LoadAwarePartitionAssignor extends AbstractPartitionAssignor implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(LoadAwarePartitionAssignor.class);

    private WeightService weightService = null;
    private BalanceService balanceService = null;
    private MemberIdTracker memberIdTracker = null;
    private String instanceId = null;
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

    /**
     * Reports this consumer's instance id to the group leader; the leader groups members
     * sharing an id into one application instance when balancing.
     */
    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return instanceId == null ? null : InstanceUserData.encode(instanceId);
    }

    private Map<String, List<TopicPartition>> assignWithLoadAwareness(
            Map<String, Integer> partitionsPerTopic,
            Map<String, Subscription> subscriptions) {

        Set<TopicPartition> allPartitions = getAllPartitions(partitionsPerTopic);
        Map<TopicPartition, Double> weights = PartitionWeights.sanitized(
                allPartitions, weightService.computeWeights(allPartitions));

        return balanceService.computeOptimalAssignment(groupMembersFrom(subscriptions), weights);
    }

    /**
     * Builds the balance-service view of the group: each member with the instance id it
     * reported through subscription userData. A member without a readable id (e.g. one
     * running an older library version during a rolling upgrade) counts as its own
     * single-member instance.
     */
    private static List<GroupMember> groupMembersFrom(Map<String, Subscription> subscriptions) {
        List<GroupMember> members = new ArrayList<>(subscriptions.size());
        List<String> absent = new ArrayList<>();
        List<String> corrupt = new ArrayList<>();
        subscriptions.forEach((memberId, subscription) -> {
            InstanceUserData.Decoded decoded = InstanceUserData.decode(subscription.userData());
            if (decoded.status() == InstanceUserData.Status.ABSENT) {
                absent.add(memberId);
            } else if (decoded.status() == InstanceUserData.Status.CORRUPT) {
                corrupt.add(memberId);
            }
            members.add(new GroupMember(
                    memberId,
                    decoded.ok() ? decoded.instanceId() : memberId,
                    Set.copyOf(subscription.topics())));
        });
        if (!absent.isEmpty()) {
            log.info("Members {} sent no instance id; treating each as its own instance"
                    + " (expected while rolling out a version that reports instance ids)", absent);
        }
        if (!corrupt.isEmpty()) {
            log.warn("Members {} sent unreadable instance-id userData; treating each as its own instance",
                    corrupt);
        }
        return members;
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
                : createDefaultWeightService(configs);

        this.memberIdTracker = ConfigInstanceResolver.resolveOrNull(
                configs, LoadAwareAssignorConfig.MEMBER_ID_TRACKER, MemberIdTracker.class);

        this.instanceId = InstanceIdResolver.resolve(LoadAwareAssignorConfig.stringConfig(
                configs, LoadAwareAssignorConfig.INSTANCE_ID, null));
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

    private static WeightService createDefaultWeightService(Map<String, ?> configs) {
        String weightStore = LoadAwareAssignorConfig.stringConfig(
                configs, LoadAwareAssignorConfig.WEIGHT_STORE, LoadAwareAssignorConfig.WEIGHT_STORE_OFFSET_RATE);
        return switch (weightStore) {
            case LoadAwareAssignorConfig.WEIGHT_STORE_OFFSET_RATE -> createDefaultOffsetRateWeightService(configs);
            case LoadAwareAssignorConfig.WEIGHT_STORE_PROMETHEUS -> createDefaultPrometheusWeightService(configs);
            default -> throw new IllegalArgumentException(
                    "Unknown " + LoadAwareAssignorConfig.WEIGHT_STORE + " value '" + weightStore
                            + "'. Supported: '" + LoadAwareAssignorConfig.WEIGHT_STORE_OFFSET_RATE
                            + "', '" + LoadAwareAssignorConfig.WEIGHT_STORE_PROMETHEUS + "'.");
        };
    }

    /**
     * Builds the offset-rate default from the consumer's own configs: every consumer
     * config that is also an admin client config (bootstrap servers, SSL/SASL, ...) is
     * reused for the store's admin client. The store — and its admin client — live for
     * the lifetime of the JVM (only daemon threads); for explicit lifecycle control,
     * configure a pre-built instance via {@link LoadAwareAssignorConfig#WEIGHT_SERVICE}.
     */
    private static KafkaOffsetRateWeightService createDefaultOffsetRateWeightService(Map<String, ?> configs) {
        Duration rateInterval = Duration.ofMillis(LoadAwareAssignorConfig.parseLong(
                configs,
                LoadAwareAssignorConfig.OFFSET_RATE_RATE_INTERVAL_MS,
                KafkaOffsetRateWeightService.DEFAULT_RATE_INTERVAL.toMillis()));
        Duration sampleInterval = configs.get(LoadAwareAssignorConfig.OFFSET_RATE_SAMPLE_INTERVAL_MS) == null
                ? null
                : Duration.ofMillis(LoadAwareAssignorConfig.parseLong(
                        configs, LoadAwareAssignorConfig.OFFSET_RATE_SAMPLE_INTERVAL_MS, 0L));
        return KafkaOffsetRateWeightService.withOwnAdminClient(
                LoadAwareAssignorConfig.adminClientConfigsFrom(configs), rateInterval, sampleInterval);
    }

    private static PrometheusWeightService createDefaultPrometheusWeightService(Map<String, ?> configs) {
        PrometheusConnectionSettings settings = LoadAwareAssignorConfig.connectionSettingsFrom(configs);
        var prometheusClient = new PrometheusClient(settings, PrometheusObjectMappers.create());
        String weightQueryTemplate = requireNonBlank(
                configs,
                LoadAwareAssignorConfig.PROMETHEUS_WEIGHT_QUERY_TEMPLATE,
                "Required when using the load-aware assignor with assignor.load-aware.weight-store="
                        + LoadAwareAssignorConfig.WEIGHT_STORE_PROMETHEUS
                        + ": assignor.load-aware.prometheus.weight-query-template "
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
         * implementation gets {@code configure(configs)} called). When absent, the default
         * selected by {@link #WEIGHT_STORE} applies.
         */
        public static final String WEIGHT_SERVICE = "assignor.load-aware.weight-service";
        /**
         * Built-in weight store used when {@link #WEIGHT_SERVICE} is absent:
         * {@link #WEIGHT_STORE_OFFSET_RATE} (default) or {@link #WEIGHT_STORE_PROMETHEUS}.
         */
        public static final String WEIGHT_STORE = "assignor.load-aware.weight-store";
        /**
         * {@link KafkaOffsetRateWeightService} built on an admin client that reuses the
         * consumer's own {@code bootstrap.servers} and security configs — needs no
         * further configuration.
         */
        public static final String WEIGHT_STORE_OFFSET_RATE = "offset-rate";
        /**
         * {@link PrometheusWeightService} built from the
         * {@code assignor.load-aware.prometheus.*} configs.
         */
        public static final String WEIGHT_STORE_PROMETHEUS = "prometheus";

        /**
         * Window (in milliseconds) over which the offset-rate store turns end-offset
         * growth into events/sec. Default: 60000.
         */
        public static final String OFFSET_RATE_RATE_INTERVAL_MS = "assignor.load-aware.offset-rate.rate-interval-ms";
        /**
         * How often (in milliseconds) the offset-rate store samples end offsets in the
         * background. Default: a quarter of the rate interval, clamped between 1 and 30
         * seconds.
         */
        public static final String OFFSET_RATE_SAMPLE_INTERVAL_MS = "assignor.load-aware.offset-rate.sample-interval-ms";
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
        /**
         * Optional application-instance id shared by every consumer in this JVM (pod).
         * Members reporting the same id are balanced as one instance: traffic is evened
         * across instances first, across each instance's members second. Default: a
         * random id generated once per JVM; set an explicit id for human-readable
         * instance labels in the leader's assignment logs.
         */
        public static final String INSTANCE_ID = "assignor.load-aware.instance-id";

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

        /**
         * Consumer configs that are also admin client configs (bootstrap servers,
         * SSL/SASL, timeouts, ...), for the offset-rate store's own admin client. The
         * consumer's {@code client.id} is dropped so the admin client generates its own —
         * reusing it would collide (e.g. in JMX) when several consumers share one id.
         */
        public static Map<String, Object> adminClientConfigsFrom(Map<String, ?> configs) {
            Map<String, Object> adminConfigs = new HashMap<>();
            for (String name : AdminClientConfig.configNames()) {
                Object value = configs.get(name);
                if (value != null) {
                    adminConfigs.put(name, value);
                }
            }
            adminConfigs.remove(AdminClientConfig.CLIENT_ID_CONFIG);
            if (adminConfigs.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
                throw new IllegalArgumentException(
                        "Required when using the load-aware assignor with the default offset-rate weight store:"
                                + " bootstrap.servers"
                                + " (not required when assignor.load-aware.weight-service is set)");
            }
            return adminConfigs;
        }

        public static PrometheusConnectionSettings connectionSettingsFrom(Map<String, ?> configs) {
            Object hostObj = configs.get(PROMETHEUS_HOST);
            if (hostObj == null || hostObj.toString().isBlank()) {
                throw new IllegalArgumentException(
                        "Required when using the load-aware assignor with assignor.load-aware.weight-store="
                                + WEIGHT_STORE_PROMETHEUS + ": assignor.load-aware.prometheus.host"
                                + " (not required when assignor.load-aware.weight-service is set)");
            }
            String host = hostObj.toString();

            Object portObj = configs.get(PROMETHEUS_PORT);
            if (portObj == null || portObj.toString().isBlank()) {
                throw new IllegalArgumentException(
                        "Required when using the load-aware assignor with assignor.load-aware.weight-store="
                                + WEIGHT_STORE_PROMETHEUS + ": assignor.load-aware.prometheus.port"
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
