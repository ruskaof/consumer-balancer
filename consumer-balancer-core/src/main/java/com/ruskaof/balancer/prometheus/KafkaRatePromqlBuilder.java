package com.ruskaof.balancer.prometheus;

import java.util.List;
import java.util.regex.Pattern;

public abstract class KafkaRatePromqlBuilder {
    protected List<String> topics;

    private final static Pattern topicValidChars = Pattern.compile("[a-zA-Z0-9._-]");

    public KafkaRatePromqlBuilder setTopics(List<String> topics) {
        if (!topics.stream().allMatch(topicValidChars.asPredicate())) {
            throw new IllegalArgumentException("Invalid Kafka topic naming. Should match regex "
                    + topicValidChars.pattern());
        }

        this.topics = topics;

        return this;
    }

    public abstract String build();
}
