package io.github.ruskaof.balancer.weight;

import io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import io.github.ruskaof.balancer.prometheus.model.PromqlResponse;
import io.github.ruskaof.balancer.prometheus.model.PrometheusDataResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.*;

/**
 * Loads per-partition weights from Prometheus. Series must carry a topic and a partition
 * label — {@code topic} and {@code partition} by default, configurable via
 * {@link #PrometheusWeightService(KafkaRatePromqlBuilder, PrometheusClient, String, String)};
 * series for partitions outside the requested set are ignored.
 * Partitions missing from the query result — including partitions whose sample is
 * {@code NaN} or infinite — receive {@link #DEFAULT_MISSING_WEIGHT} so assignment and
 * threshold logic stay well-defined.
 */
@Slf4j
public class PrometheusWeightService implements WeightService {

    /**
     * Used when Prometheus returns no usable sample for a topic/partition (no metric yet,
     * scrape gap, non-finite value, etc.).
     */
    public static final double DEFAULT_MISSING_WEIGHT = PartitionWeightDefaults.MISSING;

    public static final String DEFAULT_TOPIC_LABEL = "topic";
    public static final String DEFAULT_PARTITION_LABEL = "partition";

    private final KafkaRatePromqlBuilder kafkaRatePromqlBuilder;
    private final PrometheusClient prometheusClient;
    private final String topicLabel;
    private final String partitionLabel;

    public PrometheusWeightService(KafkaRatePromqlBuilder kafkaRatePromqlBuilder, PrometheusClient prometheusClient) {
        this(kafkaRatePromqlBuilder, prometheusClient, DEFAULT_TOPIC_LABEL, DEFAULT_PARTITION_LABEL);
    }

    /**
     * @param topicLabel     label carrying the topic name on every series of the weight query
     * @param partitionLabel label carrying the partition number on every series of the weight query
     */
    public PrometheusWeightService(
            KafkaRatePromqlBuilder kafkaRatePromqlBuilder,
            PrometheusClient prometheusClient,
            String topicLabel,
            String partitionLabel) {
        this.kafkaRatePromqlBuilder = kafkaRatePromqlBuilder;
        this.prometheusClient = prometheusClient;
        this.topicLabel = requireNonBlank(topicLabel, "topicLabel");
        this.partitionLabel = requireNonBlank(partitionLabel, "partitionLabel");
    }

    public String getTopicLabel() {
        return topicLabel;
    }

    public String getPartitionLabel() {
        return partitionLabel;
    }

    @Override
    public Map<TopicPartition, Double> computeWeights(Set<TopicPartition> allPartitions) {
        if (allPartitions.isEmpty()) {
            return Map.of();
        }

        List<String> allTopics = allPartitions.stream().map(TopicPartition::topic).distinct().toList();
        String promql = kafkaRatePromqlBuilder.build(allTopics);

        final PromqlResponse response;
        try {
            response = prometheusClient.getInstantValue(promql);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while querying Prometheus", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to query Prometheus for partition weights. promql=" + promql, e);
        }

        Map<TopicPartition, Double> result = new HashMap<>();
        if (response.getData() != null && response.getData().getResult() != null) {
            for (var series : response.getData().getResult()) {
                TopicPartition tp = topicPartitionOf(series, promql);
                if (!allPartitions.contains(tp)) {
                    log.debug("Ignoring Prometheus series for {} outside the requested partition set", tp);
                    continue;
                }
                double value = sampleValueOf(series, tp, promql);
                if (!Double.isFinite(value)) {
                    log.warn("Non-finite Prometheus sample for {}; using default weight {}",
                            tp, DEFAULT_MISSING_WEIGHT);
                    result.put(tp, DEFAULT_MISSING_WEIGHT);
                    continue;
                }
                result.put(tp, value);
            }
        }

        for (TopicPartition tp : allPartitions) {
            if (!result.containsKey(tp)) {
                log.warn("No Prometheus sample for {}; using default weight {}", tp, DEFAULT_MISSING_WEIGHT);
                result.put(tp, DEFAULT_MISSING_WEIGHT);
            }
        }

        log.debug("Computed weights: {}", result);
        return result;
    }

    private TopicPartition topicPartitionOf(PrometheusDataResult series, String promql) {
        Map<String, String> labels = series.getMetric();
        String topic = labels == null ? null : labels.get(topicLabel);
        String partition = labels == null ? null : labels.get(partitionLabel);
        if (topic == null || partition == null) {
            throw new IllegalStateException(
                    "Prometheus series is missing the '" + topicLabel + "' or '" + partitionLabel
                            + "' label. The weight query must keep both labels on every series,"
                            + " e.g. 'sum by (" + topicLabel + ", " + partitionLabel + ") (...)'."
                            + " labels=" + labels + " promql=" + promql);
        }
        try {
            return new TopicPartition(topic, Integer.parseInt(partition));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Prometheus series has a non-numeric '" + partitionLabel + "' label '" + partition
                            + "'. labels=" + labels + " promql=" + promql, e);
        }
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static double sampleValueOf(PrometheusDataResult series, TopicPartition tp, String promql) {
        if (series.getValue() == null || series.getValue().getValue() == null) {
            throw new IllegalStateException(
                    "Prometheus series for " + tp + " has no instant value. The weight query must"
                            + " return an instant vector (range/matrix results are not supported)."
                            + " promql=" + promql);
        }
        String raw = series.getValue().getValue();
        // Prometheus encodes special values as "NaN", "+Inf" and "-Inf", which
        // Double.parseDouble does not accept.
        return switch (raw) {
            case "NaN" -> Double.NaN;
            case "+Inf", "Inf" -> Double.POSITIVE_INFINITY;
            case "-Inf" -> Double.NEGATIVE_INFINITY;
            default -> {
                try {
                    yield Double.parseDouble(raw);
                } catch (NumberFormatException e) {
                    throw new IllegalStateException(
                            "Prometheus series for " + tp + " has a non-numeric sample value '"
                                    + raw + "'. promql=" + promql, e);
                }
            }
        };
    }
}
