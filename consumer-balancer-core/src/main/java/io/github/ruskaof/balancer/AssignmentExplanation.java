package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Read model of one computed assignment: the per-instance distribution plus the detected
 * causes of unevenness, formatted for the assignor's log. Only computes and formats — the
 * assignor decides what to log at which level. Tolerates output from custom
 * {@link BalanceService} implementations: members the group does not know become their own
 * instance, partitions outside the weight map count at the default weight, and partitions
 * left unassigned are reported as an unevenness factor.
 */
final class AssignmentExplanation {

    /**
     * A defaulted weight no longer moves placement decisions once the heaviest measured
     * weight is this many times larger; the explanation calls that out.
     */
    static final double DEFAULT_DWARFED_FACTOR = 100.0;

    private static final int MAX_LISTED = 5;
    private static final Comparator<TopicPartition> PARTITION_ORDER =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    private final Map<TopicPartition, Double> weights;
    private final int defaultedCount;
    private final SortedMap<String, SortedSet<String>> membersByInstance;
    private final SortedMap<String, List<TopicPartition>> partitionsByMember;
    private final SortedMap<String, Double> loadByMember;
    private final List<InstanceLine> instances;
    private final double totalWeight;
    private final String skewLine;
    private final List<String> reasons;

    /** One instance's share of the distribution, in instance-id order. */
    record InstanceLine(String instanceId, int memberCount, int partitionCount, double load) {
    }

    static AssignmentExplanation explain(
            Collection<GroupMember> members,
            Map<TopicPartition, Double> weights,
            int defaultedCount,
            Map<String, List<TopicPartition>> assignment) {
        return new AssignmentExplanation(members, weights, defaultedCount, assignment);
    }

    private AssignmentExplanation(
            Collection<GroupMember> members,
            Map<TopicPartition, Double> weights,
            int defaultedCount,
            Map<String, List<TopicPartition>> assignment) {
        this.weights = weights;
        this.defaultedCount = defaultedCount;

        SortedMap<String, String> instanceByMember = new TreeMap<>();
        for (GroupMember member : members) {
            instanceByMember.put(member.memberId(), member.instanceId());
        }
        for (String memberId : assignment.keySet()) {
            instanceByMember.putIfAbsent(memberId, memberId);
        }

        this.membersByInstance = new TreeMap<>();
        instanceByMember.forEach((memberId, instanceId) -> membersByInstance
                .computeIfAbsent(instanceId, id -> new TreeSet<>())
                .add(memberId));

        this.partitionsByMember = new TreeMap<>();
        this.loadByMember = new TreeMap<>();
        for (String memberId : instanceByMember.keySet()) {
            List<TopicPartition> partitions = new ArrayList<>(assignment.getOrDefault(memberId, List.of()));
            partitions.sort(PARTITION_ORDER);
            partitionsByMember.put(memberId, partitions);
            loadByMember.put(memberId, partitions.stream().mapToDouble(this::weightOf).sum());
        }

        this.totalWeight = weights.values().stream().mapToDouble(Double::doubleValue).sum();

        List<InstanceLine> lines = new ArrayList<>(membersByInstance.size());
        membersByInstance.forEach((instanceId, instanceMembers) -> lines.add(new InstanceLine(
                instanceId,
                instanceMembers.size(),
                instanceMembers.stream().mapToInt(m -> partitionsByMember.get(m).size()).sum(),
                instanceMembers.stream().mapToDouble(loadByMember::get).sum())));
        this.instances = List.copyOf(lines);

        this.skewLine = computeSkewLine();
        this.reasons = detectReasons(members, assignment);
    }

    List<InstanceLine> instances() {
        return instances;
    }

    List<String> reasons() {
        return reasons;
    }

    /** The compact per-instance summary with detected unevenness factors, one INFO event. */
    String infoSummary() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Load-aware assignment computed [instances=").append(instances.size())
                .append(", members=").append(partitionsByMember.size())
                .append(", partitions=").append(weights.size())
                .append(", totalWeight=").append(fmt(totalWeight))
                .append(", defaultedWeights=").append(defaultedCount)
                .append("]:");
        for (InstanceLine line : instances) {
            sb.append("\n  instance ").append(line.instanceId())
                    .append(": load=").append(fmt(line.load()));
            if (totalWeight > 0.0) {
                sb.append(String.format(Locale.ROOT, " (%.1f%% of total)", line.load() / totalWeight * 100.0));
            }
            sb.append(", partitions=").append(line.partitionCount())
                    .append(", members=").append(line.memberCount());
        }
        sb.append("\n  ").append(skewLine);
        if (reasons.isEmpty()) {
            sb.append("\n  unevenness factors: none detected");
        } else {
            sb.append("\n  unevenness factors:");
            for (String reason : reasons) {
                sb.append("\n  - ").append(reason);
            }
        }
        return sb.toString();
    }

    /** The full member → partitions-with-weights table, one DEBUG event. */
    String debugTable() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Load-aware assignment detail [instances=").append(instances.size())
                .append(", members=").append(partitionsByMember.size())
                .append(", partitions=").append(weights.size())
                .append("]:");
        membersByInstance.forEach((instanceId, instanceMembers) -> {
            for (String memberId : instanceMembers) {
                sb.append("\n  ").append(instanceId).append('/').append(memberId)
                        .append(": load=").append(fmt(loadByMember.get(memberId)))
                        .append(", partitions=[");
                StringJoiner partitions = new StringJoiner(", ");
                for (TopicPartition partition : partitionsByMember.get(memberId)) {
                    partitions.add(partition + "=" + fmt(weightOf(partition)));
                }
                sb.append(partitions).append(']');
            }
        });
        return sb.toString();
    }

    /**
     * One INFO line describing a round-robin fallback result per instance; the failure that
     * caused the fallback is on the warning logged just before it.
     */
    static String fallbackSummary(
            Collection<GroupMember> members,
            Map<String, List<TopicPartition>> fallbackAssignment) {
        SortedMap<String, String> instanceByMember = new TreeMap<>();
        for (GroupMember member : members) {
            instanceByMember.put(member.memberId(), member.instanceId());
        }
        for (String memberId : fallbackAssignment.keySet()) {
            instanceByMember.putIfAbsent(memberId, memberId);
        }
        SortedMap<String, Integer> partitionsByInstance = new TreeMap<>();
        SortedMap<String, Integer> membersByInstance = new TreeMap<>();
        instanceByMember.forEach((memberId, instanceId) -> {
            partitionsByInstance.merge(
                    instanceId, fallbackAssignment.getOrDefault(memberId, List.of()).size(), Integer::sum);
            membersByInstance.merge(instanceId, 1, Integer::sum);
        });
        StringJoiner joined = new StringJoiner(", ");
        partitionsByInstance.forEach((instanceId, partitionCount) -> joined.add(
                instanceId + "=" + plural(partitionCount, "partition")
                        + " (" + plural(membersByInstance.get(instanceId), "member") + ")"));
        return "Round-robin fallback distribution (partition weights were ignored;"
                + " see the preceding warning for the cause): " + joined;
    }

    private String computeSkewLine() {
        if (instances.size() <= 1) {
            return "skew: n/a (single instance)";
        }
        if (totalWeight <= 0.0) {
            return "skew: n/a (no measured load)";
        }
        InstanceLine max = instances.stream()
                .max(Comparator.comparingDouble(InstanceLine::load))
                .orElseThrow();
        double idealShare = totalWeight / instances.size();
        double percentAboveIdeal = (max.load() / idealShare - 1.0) * 100.0;
        return String.format(Locale.ROOT, "skew: max instance load %s (%s) is %+.1f%% above the ideal even share %s",
                fmt(max.load()), max.instanceId(), percentAboveIdeal, fmt(idealShare));
    }

    private List<String> detectReasons(
            Collection<GroupMember> members,
            Map<String, List<TopicPartition>> assignment) {
        List<String> found = new ArrayList<>();
        int partitionCount = weights.size();
        int instanceCount = membersByInstance.size();
        double maxWeight = weights.values().stream().mapToDouble(Double::doubleValue).max().orElse(0.0);

        Set<TopicPartition> assigned = new HashSet<>();
        assignment.values().forEach(assigned::addAll);
        List<TopicPartition> unassigned = weights.keySet().stream()
                .filter(partition -> !assigned.contains(partition))
                .sorted(PARTITION_ORDER)
                .toList();
        if (!unassigned.isEmpty()) {
            found.add(unassigned.size() + " partition(s) were not assigned to any member — unexpected;"
                    + " check the configured balance service: "
                    + "[" + cappedJoin(unassigned.stream().map(TopicPartition::toString).toList()) + "]");
        }

        if (defaultedCount == partitionCount && partitionCount > 0) {
            found.add("all " + partitionCount + " partitions had no usable weight and got the default "
                    + fmt(PartitionWeightDefaults.MISSING) + " — the distribution is by partition count, not load"
                    + " (expected on the first assignment after startup, before the weight store has history)");
        } else if (defaultedCount > 0) {
            found.add(defaultedCount + " of " + partitionCount + " partitions had no usable weight and got"
                    + " the default " + fmt(PartitionWeightDefaults.MISSING)
                    + "; their real load is invisible to this assignment");
            if (maxWeight >= DEFAULT_DWARFED_FACTOR * PartitionWeightDefaults.MISSING) {
                found.add("the default weight " + fmt(PartitionWeightDefaults.MISSING)
                        + " is negligible next to the heaviest measured weight " + fmt(maxWeight)
                        + " (>=" + (int) DEFAULT_DWARFED_FACTOR + "x), so defaulted partitions were placed"
                        + " as if nearly free");
            }
        }

        if (partitionCount > 0 && maxWeight <= 0.0) {
            found.add("every partition weight is zero — partitions were spread by count, not load"
                    + " (the weight store currently reports no traffic)");
        }

        if (instanceCount > 1 && maxWeight > 0.0) {
            double idealShare = totalWeight / instanceCount;
            if (maxWeight > idealShare) {
                TopicPartition heaviest = weights.entrySet().stream()
                        .filter(entry -> entry.getValue().doubleValue() == maxWeight)
                        .map(Map.Entry::getKey)
                        .min(PARTITION_ORDER)
                        .orElseThrow();
                found.add("partition " + heaviest + " alone weighs " + fmt(maxWeight)
                        + ", more than the ideal even share " + fmt(idealShare)
                        + " — a perfectly even distribution is impossible");
            }
        }

        Map<String, Set<String>> eligibleInstancesByTopic = new HashMap<>();
        for (GroupMember member : members) {
            for (String topic : member.subscribedTopics()) {
                eligibleInstancesByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(member.instanceId());
            }
        }
        SortedSet<String> assignedTopics = new TreeSet<>();
        assigned.forEach(partition -> assignedTopics.add(partition.topic()));
        List<String> constrainedTopics = new ArrayList<>();
        for (String topic : assignedTopics) {
            Set<String> eligible = eligibleInstancesByTopic.getOrDefault(topic, Set.of());
            if (!eligible.isEmpty() && eligible.size() < instanceCount) {
                double topicWeight = weights.entrySet().stream()
                        .filter(entry -> entry.getKey().topic().equals(topic))
                        .mapToDouble(Map.Entry::getValue)
                        .sum();
                constrainedTopics.add(topic + " (" + eligible.size() + " of " + instanceCount
                        + " instances, weight " + fmt(topicWeight) + ")");
            }
        }
        if (!constrainedTopics.isEmpty()) {
            found.add("partitions of " + constrainedTopics.size()
                    + " topic(s) could only go to a subset of instances: " + cappedJoin(constrainedTopics));
        }

        if (partitionCount > 0 && instanceCount > partitionCount) {
            List<String> emptyInstances = instances.stream()
                    .filter(line -> line.partitionCount() == 0)
                    .map(InstanceLine::instanceId)
                    .toList();
            found.add("more instances (" + instanceCount + ") than partitions (" + partitionCount
                    + ") — instance(s) [" + cappedJoin(emptyInstances) + "] received nothing");
        }

        if (instanceCount > 1) {
            IntSummaryStatistics memberCounts = instances.stream()
                    .mapToInt(InstanceLine::memberCount)
                    .summaryStatistics();
            if (memberCounts.getMin() != memberCounts.getMax()) {
                found.add("instances run different member counts (" + memberCounts.getMin() + "-"
                        + memberCounts.getMax() + "); each instance still receives an equal load share"
                        + " by design, so members of smaller instances run hotter");
            }
        }
        return List.copyOf(found);
    }

    private double weightOf(TopicPartition partition) {
        return weights.getOrDefault(partition, PartitionWeightDefaults.MISSING);
    }

    private static String cappedJoin(List<String> items) {
        if (items.size() <= MAX_LISTED) {
            return String.join(", ", items);
        }
        return String.join(", ", items.subList(0, MAX_LISTED))
                + ", and " + (items.size() - MAX_LISTED) + " more";
    }

    private static String plural(int count, String noun) {
        return count + " " + noun + (count == 1 ? "" : "s");
    }

    static String fmt(double value) {
        if (Math.abs(value) >= 1000.0) {
            return String.format(Locale.ROOT, "%.0f", value);
        }
        if (Math.abs(value) >= 1.0 || value == 0.0) {
            return String.format(Locale.ROOT, "%.1f", value);
        }
        return String.format(Locale.ROOT, "%.3f", value);
    }
}
