package io.github.ruskaof.balancer.prometheus;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

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

    public abstract String build();
}
