package io.github.ruskaof.balancer.prometheus;

import java.util.Objects;
import java.util.StringJoiner;

public class TemplatedKafkaRatePromqlBuilder extends KafkaRatePromqlBuilder {

    private final String template;

    public TemplatedKafkaRatePromqlBuilder(String template) {
        if (template == null || template.isBlank()) {
            throw new IllegalArgumentException("PromQL template is required and must not be blank");
        }
        if (!template.contains("%s")) {
            throw new IllegalArgumentException(
                    "PromQL template must contain the %s placeholder for the topic regex list");
        }
        this.template = template;
    }

    @Override
    public String build() {
        Objects.requireNonNull(topics, "topics");
        StringJoiner topicsListRegex = new StringJoiner("|");
        for (String topic : topics) {
            topicsListRegex.add(escapeTopicForRegex(topic));
        }
        return String.format(template, topicsListRegex);
    }

    private static String escapeTopicForRegex(String topic) {
        // '.' is the only regex metacharacter valid in Kafka topic names (a-z A-Z 0-9 . _ -)
        return topic.replace(".", "\\.");
    }
}
