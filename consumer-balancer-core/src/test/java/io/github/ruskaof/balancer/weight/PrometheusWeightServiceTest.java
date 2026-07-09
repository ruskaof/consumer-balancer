package io.github.ruskaof.balancer.weight;

import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import io.github.ruskaof.balancer.prometheus.PrometheusConnectionSettings;
import io.github.ruskaof.balancer.prometheus.PrometheusObjectMappers;
import io.github.ruskaof.balancer.prometheus.TemplatedKafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.prometheus.model.InstantValue;
import io.github.ruskaof.balancer.prometheus.model.PrometheusData;
import io.github.ruskaof.balancer.prometheus.model.PrometheusDataResult;
import io.github.ruskaof.balancer.prometheus.model.PromqlResponse;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PrometheusWeightServiceTest {

    private static final TopicPartition T0 = new TopicPartition("t", 0);
    private static final TopicPartition T1 = new TopicPartition("t", 1);

    @Test
    void mapsSeriesToWeightsAndDefaultsMissingPartitions() {
        PrometheusWeightService service = service(response(series("t", "0", "2.5")));

        Map<TopicPartition, Double> weights = service.computeWeights(Set.of(T0, T1));

        assertEquals(Map.of(T0, 2.5, T1, PrometheusWeightService.DEFAULT_MISSING_WEIGHT), weights);
    }

    @Test
    void ignoresSeriesOutsideTheRequestedPartitionSet() {
        PrometheusWeightService service = service(response(
                series("t", "0", "2.5"),
                series("t", "99", "1000.0")));

        Map<TopicPartition, Double> weights = service.computeWeights(Set.of(T0, T1));

        assertEquals(Set.of(T0, T1), weights.keySet());
        assertEquals(2.5, weights.get(T0));
    }

    @Test
    void nonFiniteSamplesFallBackToTheDefaultWeight() {
        PrometheusWeightService service = service(response(
                series("t", "0", "NaN"),
                series("t", "1", "+Inf")));

        Map<TopicPartition, Double> weights = service.computeWeights(Set.of(T0, T1));

        assertEquals(Map.of(
                T0, PrometheusWeightService.DEFAULT_MISSING_WEIGHT,
                T1, PrometheusWeightService.DEFAULT_MISSING_WEIGHT), weights);
    }

    @Test
    void failsActionablyWhenSeriesIsMissingTopicOrPartitionLabels() {
        PrometheusWeightService service = service(response(new PrometheusDataResult(
                Map.of("topic", "t"), new InstantValue(0, "1.0"))));

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> service.computeWeights(Set.of(T0)));

        assertTrue(e.getMessage().contains("sum by (topic, partition)"),
                "message should tell the user how to fix the query, was: " + e.getMessage());
    }

    @Test
    void failsActionablyWhenSeriesHasNoInstantValue() {
        PrometheusWeightService service = service(response(new PrometheusDataResult(
                Map.of("topic", "t", "partition", "0"), null)));

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> service.computeWeights(Set.of(T0)));

        assertTrue(e.getMessage().contains("instant vector"),
                "message should point at range/matrix queries, was: " + e.getMessage());
    }

    @Test
    void defaultsEverythingWhenResponseHasNoData() {
        PrometheusWeightService service = service(new PromqlResponse("success", null, null, null));

        Map<TopicPartition, Double> weights = service.computeWeights(Set.of(T0, T1));

        assertEquals(Map.of(
                T0, PrometheusWeightService.DEFAULT_MISSING_WEIGHT,
                T1, PrometheusWeightService.DEFAULT_MISSING_WEIGHT), weights);
    }

    private static PrometheusWeightService service(PromqlResponse response) {
        return new PrometheusWeightService(
                new TemplatedKafkaRatePromqlBuilder("x{topic=~\"%s\"}"),
                stubClient(response));
    }

    private static PrometheusClient stubClient(PromqlResponse response) {
        PrometheusConnectionSettings settings = new PrometheusConnectionSettings(
                "http", "localhost", 9090, "", Duration.ofSeconds(1), Duration.ofSeconds(1));
        return new PrometheusClient(settings, PrometheusObjectMappers.create()) {
            @Override
            public PromqlResponse getInstantValue(String promql) {
                return response;
            }
        };
    }

    private static PromqlResponse response(PrometheusDataResult... series) {
        return new PromqlResponse("success", new PrometheusData(List.of(series)), null, null);
    }

    private static PrometheusDataResult series(String topic, String partition, String value) {
        return new PrometheusDataResult(
                Map.of("topic", topic, "partition", partition),
                new InstantValue(0, value));
    }
}
