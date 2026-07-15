package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.weight.PrometheusWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = BalancerAutoConfigurationPrometheusLabelsTest.App.class, properties = {
        "consumer-balancer.proactive-rebalance-enabled=false",
        "consumer-balancer.weight-store=prometheus",
        "consumer-balancer.prometheus.weight-query-template="
                + "sum(rate(kafka_messages_total{kafka_topic=~\"%s\"}[1m])) by (kafka_topic, kafka_partition)",
        "consumer-balancer.prometheus.topic-label=kafka_topic",
        "consumer-balancer.prometheus.partition-label=kafka_partition"
})
class BalancerAutoConfigurationPrometheusLabelsTest {

    @Autowired
    ApplicationContext context;

    @Test
    void customLabelNamesReachThePrometheusWeightService() {
        WeightService weightService = context.getBean(WeightService.class);

        assertThat(weightService).isInstanceOf(PrometheusWeightService.class);
        PrometheusWeightService prometheusWeightService = (PrometheusWeightService) weightService;
        assertThat(prometheusWeightService.getTopicLabel()).isEqualTo("kafka_topic");
        assertThat(prometheusWeightService.getPartitionLabel()).isEqualTo("kafka_partition");
    }

    @SpringBootApplication(exclude = KafkaAutoConfiguration.class)
    static class App {

        @Bean
        org.springframework.boot.kafka.autoconfigure.KafkaProperties kafkaProperties() {
            org.springframework.boot.kafka.autoconfigure.KafkaProperties p = new org.springframework.boot.kafka.autoconfigure.KafkaProperties();
            p.setBootstrapServers(List.of("127.0.0.1:9092"));
            p.getConsumer().setGroupId("test-group");
            return p;
        }

        @Bean
        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
            return mock(KafkaListenerEndpointRegistry.class);
        }
    }
}
