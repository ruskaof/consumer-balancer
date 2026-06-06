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

    private final Prometheus prometheus = new Prometheus();
    private final Coordinator coordinator = new Coordinator();

    /**
     * Which rebalance trigger the auto-configuration wires in.
     */
    private TriggerType triggerType = TriggerType.THRESHOLD;

    /**
     * {@link TriggerType#THRESHOLD}: fire proactive rebalance when
     * (max member load / optimal max load) exceeds this value.
     */
    private double rebalanceLoadImbalanceThreshold = 1.1d;

    /**
     * {@link TriggerType#CONSUMER_LAG}: fire when the most-behind member's lag
     * exceeds this multiple of the average member lag.
     */
    private double lagImbalanceThreshold = 2.0d;

    /**
     * {@link TriggerType#CONSUMER_LAG}: minimum total group lag before the lag
     * trigger is allowed to fire, so tiny absolute lags are ignored.
     */
    private long minTotalLag = 100L;

    /**
     * {@link TriggerType#LOAD_VARIANCE}: fire when the coefficient of variation
     * (std dev / mean) of per-member load exceeds this value.
     */
    private double loadVarianceThreshold = 0.3d;

    /**
     * {@link TriggerType#PERIODIC}: rebalance on this fixed interval regardless
     * of load.
     */
    private Duration periodicTriggerInterval = Duration.ofMinutes(1);

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

    public TriggerType getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(TriggerType triggerType) {
        this.triggerType = triggerType;
    }

    public double getLagImbalanceThreshold() {
        return lagImbalanceThreshold;
    }

    public void setLagImbalanceThreshold(double lagImbalanceThreshold) {
        this.lagImbalanceThreshold = lagImbalanceThreshold;
    }

    public long getMinTotalLag() {
        return minTotalLag;
    }

    public void setMinTotalLag(long minTotalLag) {
        this.minTotalLag = minTotalLag;
    }

    public double getLoadVarianceThreshold() {
        return loadVarianceThreshold;
    }

    public void setLoadVarianceThreshold(double loadVarianceThreshold) {
        this.loadVarianceThreshold = loadVarianceThreshold;
    }

    public Duration getPeriodicTriggerInterval() {
        return periodicTriggerInterval;
    }

    public void setPeriodicTriggerInterval(Duration periodicTriggerInterval) {
        this.periodicTriggerInterval = periodicTriggerInterval;
    }

    public static class Prometheus {
        /**
         * Required when using the default
         * {@link io.github.ruskaof.balancer.weight.PrometheusWeightService}:
         * PromQL template with placeholder {@code %s} where the topic regex list is
         * inserted.
         * Results must include {@code topic} and {@code partition} labels on each
         * series.
         */
        private String weightQueryTemplate;

        private String host = "localhost";
        private int port = 9090;
        private String scheme = "http";
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);

        public String getWeightQueryTemplate() {
            return weightQueryTemplate;
        }

        public void setWeightQueryTemplate(String weightQueryTemplate) {
            this.weightQueryTemplate = weightQueryTemplate;
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
