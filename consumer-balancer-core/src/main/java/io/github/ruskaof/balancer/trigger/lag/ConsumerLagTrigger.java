package io.github.ruskaof.balancer.trigger.lag;

import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Kafka-native trigger that reacts to uneven consumer lag between members
 * without depending on Prometheus. Per-partition lag is {@code endOffset -
 * committedOffset}; each member's lag is the sum over its assigned partitions.
 * Fires when the most-behind member's lag exceeds
 * {@code lagImbalanceThreshold} times the average member lag.
 * <p>
 * A {@code minTotalLag} floor avoids reacting to noise when the group is
 * essentially caught up (small absolute lags can have a large ratio while being
 * operationally irrelevant). Because lag is a downstream symptom, this trigger
 * reacts only once imbalance has already begun to hurt throughput.
 */
@Slf4j
@RequiredArgsConstructor
public class ConsumerLagTrigger implements RebalanceTrigger {

    private final AdminClient adminClient;
    private final String groupId;
    private final double lagImbalanceThreshold;
    private final long minTotalLag;

    @Override
    public boolean shouldTrigger() {
        try {
            var groupDescription = adminClient.describeConsumerGroups(List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get();

            int memberCount = groupDescription.members().size();
            if (memberCount == 0) {
                return false;
            }

            Map<TopicPartition, OffsetAndMetadata> committedOffsets = adminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get();

            if (committedOffsets.isEmpty()) {
                return false;
            }

            Map<TopicPartition, OffsetSpec> latestSpecs = committedOffsets.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResultInfo> endOffsets = adminClient
                    .listOffsets(latestSpecs)
                    .all()
                    .get();

            Map<TopicPartition, Long> lagByPartition = new HashMap<>();
            for (var entry : committedOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                ListOffsetsResultInfo end = endOffsets.get(tp);
                if (end == null) {
                    continue;
                }
                long lag = Math.max(0L, end.offset() - entry.getValue().offset());
                lagByPartition.put(tp, lag);
            }

            long totalLag = lagByPartition.values().stream().mapToLong(Long::longValue).sum();
            if (totalLag < minTotalLag) {
                log.info("ConsumerLagTrigger: below floor (totalLag={}, minTotalLag={})", totalLag, minTotalLag);
                return false;
            }

            long maxMemberLag = 0L;
            for (var member : groupDescription.members()) {
                long memberLag = 0L;
                for (TopicPartition tp : member.assignment().topicPartitions()) {
                    memberLag += lagByPartition.getOrDefault(tp, 0L);
                }
                maxMemberLag = Math.max(maxMemberLag, memberLag);
            }

            double meanMemberLag = (double) totalLag / memberCount;
            boolean shouldTrigger = maxMemberLag > meanMemberLag * lagImbalanceThreshold;

            log.info("ConsumerLagTrigger result: totalLag={}, maxMemberLag={}, meanMemberLag={}, shouldTrigger={}",
                    totalLag, maxMemberLag, meanMemberLag, shouldTrigger);
            return shouldTrigger;
        } catch (Exception e) {
            log.error("Could not run ConsumerLagTrigger", e);
            return false;
        }
    }
}
