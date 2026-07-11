package io.github.ruskaof.balancer.prometheus;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Builds the PromQL query used to load per-partition weights.
 *
 * <p>The builder holds mutable state ({@link #setTopics(List)}); callers that may run
 * concurrently — e.g. the assignor and a rebalance trigger sharing one instance — must use
 * {@link #build(List)}, which synchronizes the set-and-build sequence.
 */
public abstract class KafkaRatePromqlBuilder {
    protected List<String> topics;

    private static final Pattern VALID_TOPIC_PATTERN = Pattern.compile("[a-zA-Z0-9._-]+");

    public KafkaRatePromqlBuilder setTopics(List<String> topics) {
        Objects.requireNonNull(topics, "topics");
        if (!topics.stream().allMatch(VALID_TOPIC_PATTERN.asMatchPredicate())) {
            throw new IllegalArgumentException("Invalid Kafka topic name. Topics must match: "
                    + VALID_TOPIC_PATTERN.pattern());
        }

        this.topics = topics;

        return this;
    }

    /**
     * Builds the query for the given topics in one thread-safe step.
     */
    public final synchronized String build(List<String> topics) {
        setTopics(topics);
        return build();
    }

    public abstract String build();
}
