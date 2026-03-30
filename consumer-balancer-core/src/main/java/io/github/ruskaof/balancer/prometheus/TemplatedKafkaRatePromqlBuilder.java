package io.github.ruskaof.balancer.prometheus;

import java.util.Objects;
import java.util.StringJoiner;

public class TemplatedKafkaRatePromqlBuilder extends KafkaRatePromqlBuilder {

    private final String template;

    public TemplatedKafkaRatePromqlBuilder(String template) {
        if (template == null || template.isBlank()) {
            throw new IllegalArgumentException("PromQL template is required and must not be blank");
        }
        this.template = template;
    }

    @Override
    public String build() {
        Objects.requireNonNull(topics, "topics");
        StringJoiner topicsListRegex = new StringJoiner("|");
        for (String topic : topics) {
            topicsListRegex.add(topic);
        }
        return String.format(template, topicsListRegex);
    }
}
