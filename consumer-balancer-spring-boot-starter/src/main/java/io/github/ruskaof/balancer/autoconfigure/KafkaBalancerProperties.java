package io.github.ruskaof.balancer.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "consumer-balancer")
public class KafkaBalancerProperties {

    /**
     * When false, auto-configuration for coordinator and default beans is skipped.
     */
    private boolean enabled = true;

    /**
     * When false, only the partition assignor path is used; coordinator election
     * and proactive rebalances are off.
     */
    private boolean proactiveRebalanceEnabled = true;

    /**
     * Built-in weight store to auto-configure when no custom WeightService bean is
     * defined. "offset-rate" (the default) measures per-partition events/sec by
     * tracking partition end offsets through the Kafka AdminClient and needs no
     * extra infrastructure; "prometheus" queries a Prometheus-compatible backend
     * and requires the consumer-balancer.prometheus.* properties.
     */
    private WeightStore weightStore = WeightStore.OFFSET_RATE;

    private final OffsetRate offsetRate = new OffsetRate();
    private final Prometheus prometheus = new Prometheus();
    private final Coordinator coordinator = new Coordinator();

    /**
     * Fire proactive rebalance when (max member load / optimal max load) exceeds
     * this value.
     */
    private double rebalanceLoadImbalanceThreshold = 1.1d;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isProactiveRebalanceEnabled() {
        return proactiveRebalanceEnabled;
    }

    public void setProactiveRebalanceEnabled(boolean proactiveRebalanceEnabled) {
        this.proactiveRebalanceEnabled = proactiveRebalanceEnabled;
    }

    public WeightStore getWeightStore() {
        return weightStore;
    }

    public void setWeightStore(WeightStore weightStore) {
        this.weightStore = weightStore;
    }

    public OffsetRate getOffsetRate() {
        return offsetRate;
    }

    public Prometheus getPrometheus() {
        return prometheus;
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }

    public double getRebalanceLoadImbalanceThreshold() {
        return rebalanceLoadImbalanceThreshold;
    }

    public void setRebalanceLoadImbalanceThreshold(double rebalanceLoadImbalanceThreshold) {
        this.rebalanceLoadImbalanceThreshold = rebalanceLoadImbalanceThreshold;
    }

    public enum WeightStore {
        /**
         * Per-partition events/sec measured from end-offset growth via the Kafka
         * AdminClient.
         */
        OFFSET_RATE,
        /**
         * Per-partition weights queried from a Prometheus-compatible backend.
         */
        PROMETHEUS
    }

    public static class OffsetRate {
        /**
         * Window over which end-offset growth is turned into an events/sec weight.
         */
        private Duration rateInterval = Duration.ofMinutes(1);

        /**
         * How often partition end offsets are sampled in the background. Defaults to
         * a quarter of rate-interval, clamped between 1 and 30 seconds.
         */
        private Duration sampleInterval;

        public Duration getRateInterval() {
            return rateInterval;
        }

        public void setRateInterval(Duration rateInterval) {
            this.rateInterval = rateInterval;
        }

        public Duration getSampleInterval() {
            return sampleInterval;
        }

        public void setSampleInterval(Duration sampleInterval) {
            this.sampleInterval = sampleInterval;
        }
    }

    public static class Prometheus {
        /**
         * Required when consumer-balancer.weight-store=prometheus: PromQL template
         * with placeholder {@code %s} where the topic regex list is inserted.
         * Results must include the topic and partition labels (see
         * {@code topic-label} and {@code partition-label}) on each series.
         */
        private String weightQueryTemplate;

        /**
         * Label on the weight-query series that carries the topic name.
         */
        private String topicLabel = "topic";

        /**
         * Label on the weight-query series that carries the partition number.
         */
        private String partitionLabel = "partition";

        private String host = "localhost";
        private int port = 9090;
        private String scheme = "http";

        /**
         * Path prefix prepended to {@code /api/v1/query} for Prometheus-API-compatible
         * backends, e.g. {@code /prometheus} for single-node VictoriaMetrics or
         * {@code /select/<accountID>/prometheus} for a VictoriaMetrics cluster.
         * Empty for plain Prometheus.
         */
        private String pathPrefix = "";

        /**
         * Optional value for the {@code Authorization} header sent with every query,
         * e.g. {@code Bearer <token>} or {@code Basic <base64>}. Empty means no header.
         */
        private String authorization;

        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);

        public String getWeightQueryTemplate() {
            return weightQueryTemplate;
        }

        public void setWeightQueryTemplate(String weightQueryTemplate) {
            this.weightQueryTemplate = weightQueryTemplate;
        }

        public String getTopicLabel() {
            return topicLabel;
        }

        public void setTopicLabel(String topicLabel) {
            this.topicLabel = topicLabel;
        }

        public String getPartitionLabel() {
            return partitionLabel;
        }

        public void setPartitionLabel(String partitionLabel) {
            this.partitionLabel = partitionLabel;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getScheme() {
            return scheme;
        }

        public void setScheme(String scheme) {
            this.scheme = scheme;
        }

        public String getPathPrefix() {
            return pathPrefix;
        }

        public void setPathPrefix(String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        public String getAuthorization() {
            return authorization;
        }

        public void setAuthorization(String authorization) {
            this.authorization = authorization;
        }

        public Duration getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public Duration getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }
    }

    public static class Coordinator {
        /**
         * How often the elected instance checks whether to run the rebalance trigger.
         */
        private Duration triggerCheckInterval = Duration.ofSeconds(30);

        /**
         * Interval for coordinator election against the consumer group.
         */
        private Duration electionInterval = Duration.ofSeconds(30);

        public Duration getTriggerCheckInterval() {
            return triggerCheckInterval;
        }

        public void setTriggerCheckInterval(Duration triggerCheckInterval) {
            this.triggerCheckInterval = triggerCheckInterval;
        }

        public Duration getElectionInterval() {
            return electionInterval;
        }

        public void setElectionInterval(Duration electionInterval) {
            this.electionInterval = electionInterval;
        }
    }
}
