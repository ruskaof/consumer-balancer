package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.trigger.RebalanceInitiator;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

class ConsumerGroupBalancersTest {

    private static final RebalanceInitiator NO_OP = () -> {
    };

    private final AdminClient admin = mock(AdminClient.class);
    private final WeightService weights = partitions -> Map.of();
    private final List<ConsumerGroupBalancers> created = new ArrayList<>();

    @AfterEach
    void closeRegistries() {
        created.forEach(ConsumerGroupBalancers::close);
    }

    @Test
    void assignorConfigsCarryTheSharedCollaborators() {
        BalanceService balance = (members, partitionWeights) -> Map.of();
        ConsumerGroupBalancers balancers = track(ConsumerGroupBalancers.builder()
                .adminClient(admin)
                .weightService(weights)
                .balanceService(balance)
                .instanceId("pod-a")
                .build());

        assertEquals(Map.of(
                        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LoadAwarePartitionAssignor.class.getName(),
                        LoadAwareAssignorConfig.WEIGHT_SERVICE, weights,
                        LoadAwareAssignorConfig.BALANCE_SERVICE, balance,
                        LoadAwareAssignorConfig.MEMBER_ID_TRACKER, balancers.getMemberIdTracker(),
                        LoadAwareAssignorConfig.INSTANCE_ID, "pod-a"),
                balancers.assignorConfigs());
    }

    @Test
    void assignorConfigsLeaveTheInstanceIdToTheAssignorWhenUnset() {
        ConsumerGroupBalancers balancers = registry();

        assertFalse(balancers.assignorConfigs().containsKey(LoadAwareAssignorConfig.INSTANCE_ID));
    }

    @Test
    void everyGroupTriggerReadsTheTrackerTheAssignorWritesTo() throws Exception {
        // A second tracker anywhere in this wiring would leave the trigger with an empty
        // mapping forever, and it would silently skip every check.
        ConsumerGroupBalancers balancers = registry();

        ConsumerGroupBalancer orders = balancers.register("orders", NO_OP);
        ConsumerGroupBalancer payments = balancers.register("payments", NO_OP);

        assertSame(balancers.getMemberIdTracker(), trackerOf(orders.getTrigger()));
        assertSame(balancers.getMemberIdTracker(), trackerOf(payments.getTrigger()));
        assertSame(balancers.getMemberIdTracker(),
                balancers.assignorConfigs().get(LoadAwareAssignorConfig.MEMBER_ID_TRACKER));
    }

    @Test
    void groupsUseTheRegistryDefaultsUnlessOverridden() {
        ConsumerGroupBalancers balancers = track(ConsumerGroupBalancers.builder()
                .adminClient(admin)
                .weightService(weights)
                .imbalanceThreshold(1.2)
                .build());

        ConsumerGroupBalancer orders = balancers.register("orders", NO_OP);
        ConsumerGroupBalancer payments = balancers.group("payments")
                .rebalanceInitiator(NO_OP)
                .imbalanceThreshold(1.5)
                .register();

        assertEquals(1.2, ((ThresholdTrigger) orders.getTrigger()).status().threshold());
        assertEquals(1.5, ((ThresholdTrigger) payments.getTrigger()).status().threshold());
    }

    @Test
    void aCustomTriggerReplacesTheThresholdTrigger() {
        RebalanceTrigger custom = () -> false;

        ConsumerGroupBalancer group = registry().group("orders")
                .rebalanceInitiator(NO_OP)
                .trigger(custom)
                .register();

        assertSame(custom, group.getTrigger());
        assertSame(NO_OP, group.getRebalanceInitiator());
        assertTrue(group.isProactive());
    }

    @Test
    void rejectsASecondRegistrationOfTheSameGroup() {
        ConsumerGroupBalancers balancers = registry();
        balancers.register("orders", NO_OP);

        assertThrows(IllegalArgumentException.class, () -> balancers.register("orders", NO_OP));
        assertEquals(1, balancers.getGroups().size());
    }

    @Test
    void requiresAnInitiatorWhileProactive() {
        ConsumerGroupBalancers balancers = registry();

        assertThrows(NullPointerException.class, () -> balancers.register("orders", null));
        assertTrue(balancers.getGroup("orders").isEmpty());
    }

    @Test
    void passiveGroupsNeedNoInitiatorAndRunNothing() {
        ConsumerGroupBalancers balancers = track(ConsumerGroupBalancers.builder()
                .adminClient(admin)
                .weightService(weights)
                .proactiveRebalance(false)
                .build());

        ConsumerGroupBalancer group = balancers.register("orders", null);
        balancers.start();

        assertFalse(group.isProactive());
        assertFalse(group.isCoordinator());
        assertNull(group.getTrigger());
        assertFalse(balancers.assignorConfigs().containsKey(LoadAwareAssignorConfig.MEMBER_ID_TRACKER));
        verify(admin, after(200).never()).describeConsumerGroups(anyCollection());
    }

    @Test
    void groupsRegisteredBeforeStartBeginOnStart() {
        ConsumerGroupBalancers balancers = registry();
        balancers.register("orders", NO_OP);

        verify(admin, after(200).never()).describeConsumerGroups(anyCollection());

        balancers.start();

        verify(admin, timeout(5_000).atLeastOnce()).describeConsumerGroups(List.of("orders"));
    }

    @Test
    void groupsRegisteredAfterStartBeginImmediately() {
        ConsumerGroupBalancers balancers = registry();
        balancers.start();

        balancers.register("orders", NO_OP);

        verify(admin, timeout(5_000).atLeastOnce()).describeConsumerGroups(List.of("orders"));
    }

    @Test
    void onRegisterReplaysExistingGroupsAndFollowsNewOnes() {
        ConsumerGroupBalancers balancers = registry();
        balancers.register("orders", NO_OP);
        List<String> seen = new ArrayList<>();

        balancers.onRegister(group -> seen.add(group.getGroupId()));
        balancers.register("payments", NO_OP);

        assertEquals(List.of("orders", "payments"), seen);
    }

    @Test
    void aClosedRegistryRefusesNewGroupsAndRestarts() {
        ConsumerGroupBalancers balancers = registry();
        balancers.start();
        balancers.close();

        assertFalse(balancers.isRunning());
        assertThrows(IllegalStateException.class, () -> balancers.register("orders", NO_OP));
        assertThrows(IllegalStateException.class, balancers::start);
    }

    @Test
    void requiresTheSharedCollaborators() {
        assertThrows(NullPointerException.class,
                () -> ConsumerGroupBalancers.builder().weightService(weights).build());
        assertThrows(NullPointerException.class,
                () -> ConsumerGroupBalancers.builder().adminClient(admin).build());
    }

    private ConsumerGroupBalancers registry() {
        return track(ConsumerGroupBalancers.builder().adminClient(admin).weightService(weights).build());
    }

    private ConsumerGroupBalancers track(ConsumerGroupBalancers balancers) {
        created.add(balancers);
        return balancers;
    }

    private static Object trackerOf(RebalanceTrigger trigger) throws Exception {
        Field field = ThresholdTrigger.class.getDeclaredField("memberIdTracker");
        field.setAccessible(true);
        return field.get(trigger);
    }
}
