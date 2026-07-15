package io.github.ruskaof.balancer.prometheus;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * Builds PromQL from a template in which every literal {@code %s} is replaced with the
 * {@code |}-separated, regex-escaped topic list. Other {@code %} characters in the
 * template are left untouched.
 *
 * <p>The {@code %s} placeholder must sit inside a quoted PromQL string (e.g.
 * {@code topic=~"%s"}): dots in topic names are escaped as {@code \\.} so that the
 * PromQL string parser unescapes them to the regex {@code \.}.
 */
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
        return template.replace("%s", topicsListRegex.toString());
    }

    private static String escapeTopicForRegex(String topic) {
        // '.' is the only regex metacharacter valid in Kafka topic names (a-z A-Z 0-9 . _ -).
        // The placeholder sits inside a quoted PromQL string, whose Go-style escaping
        // consumes one level of backslashes — and rejects the lone "\." as an unknown
        // escape sequence — so the query text needs "\\." to reach the regex engine as "\.".
        return topic.replace(".", "\\\\.");
    }
}
