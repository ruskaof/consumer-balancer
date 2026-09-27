package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.autoconfigure.BalancerAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.BalancerMetricsAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.DefaultBalanceServiceAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.KafkaOffsetRateWeightAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.PrometheusWeightAutoConfiguration;
import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The multi-group setup: no spring.kafka.consumer.group-id, no {@code @KafkaListener}, and one
 * consumer factory plus self-managed containers per group. Each group is registered with the
 * auto-configured balancer in one call and gets lifecycle and meters without further wiring.
 */
class BalancerMultiGroupIntegrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    KafkaAutoConfiguration.class,
                    DefaultBalanceServiceAutoConfiguration.class,
                    KafkaOffsetRateWeightAutoConfiguration.class,
                    PrometheusWeightAutoConfiguration.class,
                    BalancerAutoConfiguration.class,
                    BalancerMetricsAutoConfiguration.class))
            .withUserConfiguration(Meters.class, OrdersAndPayments.class)
            .withPropertyValues(
                    "spring.kafka.bootstrap-servers=127.0.0.1:9092",
                    // Fail fast when the elections poll the absent broker, so context shutdown
                    // does not wait for the default 60s API timeout.
                    "spring.kafka.admin.properties[default.api.timeout.ms]=1000",
                    "spring.kafka.admin.properties[request.timeout.ms]=500");

    @Test
    void registeredGroupsShareTheTrackerTheirFactoriesReportTo() {
        runner.run(context -> {
            ConsumerGroupBalancers balancers = context.getBean(ConsumerGroupBalancers.class);
            Map<String, Object> ordersConfigs = context.getBean(OrdersAndPayments.class).factories.get("orders")
                    .getConfigurationProperties();

            assertThat(balancers.getGroups()).extracting(ConsumerGroupBalancer::getGroupId)
                    .containsExactly("orders", "payments");
            assertThat(ordersConfigs.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG))
                    .isEqualTo(LoadAwarePartitionAssignor.class.getName());
            assertThat(ordersConfigs.get(LoadAwareAssignorConfig.MEMBER_ID_TRACKER))
                    .isSameAs(balancers.getMemberIdTracker());
            assertThat(balancers.isRunning()).isTrue();
        });
    }

    @Test
    void everyGroupGetsItsOwnMetersIncludingGroupsRegisteredAtRuntime() {
        runner.run(context -> {
            SimpleMeterRegistry meters = context.getBean(SimpleMeterRegistry.class);
            ConsumerGroupBalancers balancers = context.getBean(ConsumerGroupBalancers.class);

            balancers.register("audit", ContainerRebalanceInitiator.of("audit", List::of));

            for (String groupId : List.of("orders", "payments", "audit")) {
                assertThat(meters.get("consumer.balancer.coordinator").tag("group", groupId).gauge().value())
                        .isZero();
                assertThat(meters.get("consumer.balancer.trigger.imbalance.threshold")
                        .tag("group", groupId).gauge().value()).isEqualTo(1.1);
                assertThat(meters.get("consumer.balancer.rebalance.initiations")
                        .tags("group", groupId, "result", "no_match").functionCounter().count()).isZero();
            }
            // One weight store serves every group, so its meters are not per group.
            assertThat(meters.get("consumer.balancer.offset.rate.tracked.partitions").gauge().getId().getTag("group"))
                    .isNull();
        });
    }

    @Test
    void aRegistryForAnotherClusterIsStartedAndMeasuredUnderItsOwnTags() {
        runner.withUserConfiguration(ClusterB.class).run(context -> {
            SimpleMeterRegistry meters = context.getBean(SimpleMeterRegistry.class);
            ConsumerGroupBalancers clusterB = context.getBean("clusterBBalancers", ConsumerGroupBalancers.class);

            clusterB.register("orders", ContainerRebalanceInitiator.of("orders", List::of));

            assertThat(clusterB.isRunning()).isTrue();
            assertThat(meters.get("consumer.balancer.coordinator").tags("group", "orders", "cluster", "b").gauge())
                    .isNotNull();
            // The auto-configured registry keeps its own groups and its own weight store next to it.
            ConsumerGroupBalancers clusterA =
                    context.getBean(BalancerAutoConfiguration.BALANCERS_BEAN_NAME, ConsumerGroupBalancers.class);
            assertThat(clusterA.getGroups()).hasSize(2);
            assertThat(clusterA.getWeightService()).isInstanceOf(KafkaOffsetRateWeightService.class);
            assertThat(clusterB.getWeightService()).isSameAs(context.getBean("clusterBWeightService"));
        });
    }

    @Test
    void closesEveryGroupWithTheContext() {
        AtomicReference<ConsumerGroupBalancers> balancers = new AtomicReference<>();

        runner.run(context -> balancers.set(context.getBean(ConsumerGroupBalancers.class)));

        assertThat(balancers.get().isRunning()).isFalse();
    }

    @Configuration(proxyBeanMethods = false)
    static class Meters {

        @Bean
        SimpleMeterRegistry simpleMeterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    /** Stands in for the application's own per-group setup: a factory and a container manager each. */
    @Configuration(proxyBeanMethods = false)
    static class OrdersAndPayments {

        final Map<String, DefaultKafkaConsumerFactory<Object, Object>> factories = new HashMap<>();

        OrdersAndPayments(@Qualifier(BalancerAutoConfiguration.BALANCERS_BEAN_NAME) ConsumerGroupBalancers balancers) {
            for (String groupId : List.of("orders", "payments")) {
                Map<String, Object> props = new HashMap<>();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
                props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                props.putAll(balancers.assignorConfigs());
                factories.put(groupId, new DefaultKafkaConsumerFactory<>(props));

                List<MessageListenerContainer> containers = new ArrayList<>(); // the container manager's
                balancers.register(groupId, ContainerRebalanceInitiator.of(groupId, () -> containers));
            }
        }
    }

    /** The README's second-cluster recipe. */
    @Configuration(proxyBeanMethods = false)
    static class ClusterB {

        // Not default candidates, so they neither replace nor get injected into the
        // auto-configured stack of the first cluster.
        @Bean(defaultCandidate = false)
        AdminClient clusterBAdminClient() {
            return mock(AdminClient.class);
        }

        @Bean(defaultCandidate = false)
        WeightService clusterBWeightService() {
            return partitions -> Map.of();
        }

        @Bean(destroyMethod = "close")
        ConsumerGroupBalancers clusterBBalancers(
                @Qualifier("clusterBAdminClient") AdminClient adminClient,
                @Qualifier("clusterBWeightService") WeightService weightService) {
            return ConsumerGroupBalancers.builder()
                    .adminClient(adminClient)
                    .weightService(weightService)
                    .tags(Map.of("cluster", "b"))
                    .build();
        }
    }
}
