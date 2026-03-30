package com.ruskaof.balancer.autoconfigure;

import com.ruskaof.listener.prometheus.PrometheusClient;
import com.ruskaof.listener.weight.WeightService;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = BalancerAutoConfigurationCustomWeightTest.App.class, properties = "consumer-balancer.proactive-rebalance-enabled=false")
class BalancerAutoConfigurationCustomWeightTest {

    @Autowired
    ApplicationContext context;

    @Test
    void customWeightServiceReplacesDefaultPrometheusBeans() {
        assertThat(context.getBean(WeightService.class)).isNotNull();
        assertThat(context.getBeanNamesForType(PrometheusClient.class)).isEmpty();
    }

    @SpringBootApplication(exclude = KafkaAutoConfiguration.class)
    static class App {

        @Bean
        org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties() {
            org.springframework.boot.autoconfigure.kafka.KafkaProperties p = new org.springframework.boot.autoconfigure.kafka.KafkaProperties();
            p.setBootstrapServers(List.of("127.0.0.1:9092"));
            p.getConsumer().setGroupId("test-group");
            return p;
        }

        @Bean
        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
            return mock(KafkaListenerEndpointRegistry.class);
        }

        @Bean
        WeightService customWeightService() {
            return (Set<TopicPartition> partitions) -> Map.of();
        }
    }
}
