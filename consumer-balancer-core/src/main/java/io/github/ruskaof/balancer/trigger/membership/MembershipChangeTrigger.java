package io.github.ruskaof.balancer.trigger.membership;

import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MemberDescription;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Fires when the set of members in the consumer group changes (an instance
 * joined, crashed, or left) since the previous evaluation. Reacts to group
 * topology changes rather than load skew, so it complements load-based triggers
 * rather than replacing them.
 * <p>
 * The first evaluation only records a baseline and never triggers. The baseline
 * is refreshed on every evaluation, so a single membership change fires exactly
 * once. A proactive rebalance keeps existing members in the group (member ids
 * are not rotated by {@code enforceRebalance}), so triggering does not feed back
 * into itself.
 */
@Slf4j
@RequiredArgsConstructor
public class MembershipChangeTrigger implements RebalanceTrigger {

    private final AdminClient adminClient;
    private final String groupId;
    private final AtomicReference<Set<String>> lastSeenMembers = new AtomicReference<>(null);

    @Override
    public boolean shouldTrigger() {
        try {
            var groupDescription = adminClient.describeConsumerGroups(List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get();

            Set<String> currentMembers = groupDescription.members().stream()
                    .map(MemberDescription::consumerId)
                    .collect(Collectors.toUnmodifiableSet());

            Set<String> previousMembers = lastSeenMembers.getAndSet(currentMembers);

            if (previousMembers == null) {
                log.info("MembershipChangeTrigger: establishing baseline with {} members", currentMembers.size());
                return false;
            }

            boolean changed = !previousMembers.equals(currentMembers);
            log.info("MembershipChangeTrigger result: previousSize={}, currentSize={}, shouldTrigger={}",
                    previousMembers.size(), currentMembers.size(), changed);
            return changed;
        } catch (Exception e) {
            log.error("Could not run MembershipChangeTrigger", e);
            return false;
        }
    }
}
