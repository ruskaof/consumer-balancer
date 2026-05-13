package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.Map;

/**
 * Merges {@code consumer-balancer.prometheus.*} into
 * {@code spring.kafka.consumer.properties}
 * for {@link LoadAwarePartitionAssignor} so users can configure Prometheus in
 * one place.
 * Explicit {@code assignor.load-aware.prometheus.*} entries win over balancer
 * defaults.
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureBefore(KafkaAutoConfiguration.class)
@EnableConfigurationProperties(KafkaBalancerProperties.class)
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
class KafkaBalancerKafkaPropertiesMergeConfiguration {

        @Bean
        static BeanPostProcessor kafkaBalancerPrometheusConsumerPropertiesMerge(KafkaBalancerProperties balancerProperties) {
                return new BeanPostProcessor() {
                        @Override
                        public Object postProcessAfterInitialization(Object bean, String beanName)
                                        throws BeansException {
                                if (!(bean instanceof KafkaProperties kafkaProperties)) {
                                        return bean;
                                }
                                Map<String, String> props = kafkaProperties.getConsumer().getProperties();
                                props.putIfAbsent(
                                                LoadAwarePartitionAssignor.LoadAwareAssignorConfig.PROMETHEUS_HOST,
                                                balancerProperties.getPrometheus().getHost());
                                props.putIfAbsent(
                                                LoadAwarePartitionAssignor.LoadAwareAssignorConfig.PROMETHEUS_PORT,
                                                String.valueOf(balancerProperties.getPrometheus().getPort()));
                                props.putIfAbsent(
                                                LoadAwarePartitionAssignor.LoadAwareAssignorConfig.PROMETHEUS_SCHEME,
                                                balancerProperties.getPrometheus().getScheme());
                                props.putIfAbsent(
                                                LoadAwarePartitionAssignor.LoadAwareAssignorConfig.PROMETHEUS_CONNECT_TIMEOUT_MS,
                                                String.valueOf(balancerProperties.getPrometheus().getConnectTimeout()
                                                                .toMillis()));
                                props.putIfAbsent(
                                                LoadAwarePartitionAssignor.LoadAwareAssignorConfig.PROMETHEUS_REQUEST_TIMEOUT_MS,
                                                String.valueOf(balancerProperties.getPrometheus().getRequestTimeout()
                                                                .toMillis()));
                                if (balancerProperties.getPrometheus().getWeightQueryTemplate() != null
                                                && !balancerProperties.getPrometheus().getWeightQueryTemplate()
                                                                .isBlank()) {
                                        props.putIfAbsent(
                                                        LoadAwarePartitionAssignor.LoadAwareAssignorConfig.PROMETHEUS_WEIGHT_QUERY_TEMPLATE,
                                                        balancerProperties.getPrometheus().getWeightQueryTemplate());
                                }
                                return bean;
                        }
                };
        }
}
