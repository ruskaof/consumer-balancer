package io.github.ruskaof.balancer.weight;

import io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.*;

/**
 * Loads per-partition weights from Prometheus. Partitions missing from the
 * query result
 * receive {@link #DEFAULT_MISSING_WEIGHT} so assignment and threshold logic
 * stay well-defined.
 */
@Slf4j
public class PrometheusWeightService implements WeightService {

    /**
     * Used when Prometheus returns no series for a topic/partition (no metric yet,
     * scrape gap, etc.).
     */
    public static final double DEFAULT_MISSING_WEIGHT = PartitionWeightDefaults.MISSING;

    private final KafkaRatePromqlBuilder kafkaRatePromqlBuilder;
    private final PrometheusClient prometheusClient;

    public PrometheusWeightService(KafkaRatePromqlBuilder kafkaRatePromqlBuilder, PrometheusClient prometheusClient) {
        this.kafkaRatePromqlBuilder = kafkaRatePromqlBuilder;
        this.prometheusClient = prometheusClient;
    }

    @Override
    public Map<TopicPartition, Double> computeWeights(Set<TopicPartition> allPartitions) {
        if (allPartitions.isEmpty()) {
            return Map.of();
        }

        List<String> allTopics = allPartitions.stream().map(TopicPartition::topic).distinct().toList();
        String promql = kafkaRatePromqlBuilder.setTopics(allTopics).build();

        final io.github.ruskaof.balancer.prometheus.model.PromqlResponse response;
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
            for (var dataResult : response.getData().getResult()) {
                var topic = dataResult.getMetric().get("topic");
                var partition = Integer.parseInt(dataResult.getMetric().get("partition"));
                var value = Double.parseDouble(dataResult.getValue().getValue());
                result.put(new TopicPartition(topic, partition), value);
            }
        }

        for (TopicPartition tp : allPartitions) {
            if (!result.containsKey(tp) || result.get(tp) == null) {
                log.warn("No Prometheus sample for {}; using default weight {}", tp, DEFAULT_MISSING_WEIGHT);
                result.put(tp, DEFAULT_MISSING_WEIGHT);
            }
        }

        log.debug("Computed weights: {}", result);
        return result;
    }
}
