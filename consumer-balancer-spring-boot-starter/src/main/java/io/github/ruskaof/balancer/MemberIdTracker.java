package io.github.ruskaof.balancer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MemberIdTracker implements ConsumerAwareRebalanceListener {

    private final Set<String> memberIds = ConcurrentHashMap.newKeySet();

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String memberId = consumer.groupMetadata().memberId();
        String consumerGroupId = consumer.groupMetadata().groupId();

        memberIds.add(memberId);

        log.debug("Member ID registered: {} (total tracked: {}, group: {})",
                memberId, memberIds.size(), consumerGroupId);
    }

    public Set<String> getCurrentMemberIds() {
        return memberIds;
    }
}
