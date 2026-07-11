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
