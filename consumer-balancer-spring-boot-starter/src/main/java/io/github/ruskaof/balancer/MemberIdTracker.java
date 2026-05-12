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
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        memberIds.remove(consumer.groupMetadata().memberId());
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String memberId = consumer.groupMetadata().memberId();
        memberIds.add(memberId);
        log.debug("Member ID registered: {} (total tracked: {}, group: {})",
                memberId, memberIds.size(), consumer.groupMetadata().groupId());
    }

    public Set<String> getCurrentMemberIds() {
        return memberIds;
    }
}
