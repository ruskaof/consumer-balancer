package com.ruskaof.listener.prometheus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
class TemplatedKafkaRatePromqlBuilderTest {

    @Test
    void replacesTopicsPlaceholder() {
        var b = new TemplatedKafkaRatePromqlBuilder(
                "sum(rate(my_bytes_total{topic=~\"%s\"}[1m])) by (topic, partition)"
        );
        String q = b.setTopics(java.util.List.of("a", "b")).build();
        assertEquals(
                "sum(rate(my_bytes_total{topic=~\"a|b\"}[1m])) by (topic, partition)",
                q
        );
    }

    @Test
    void escapesDotInTopicForRegexLiteral() {
        var b = new TemplatedKafkaRatePromqlBuilder(
                "x{topic=~\"%s\"}y"
        );
        String q = b.setTopics(java.util.List.of("foo.bar", "baz")).build();
        assertEquals("x{topic=~\"foo\\.bar|baz\"}y", q);
    }

    @Test
    void rejectsMissingPlaceholder() {
        assertThrows(IllegalArgumentException.class, () ->
                new TemplatedKafkaRatePromqlBuilder("up")
        );
    }

    @Test
    void rejectsBlankTemplate() {
        assertThrows(IllegalArgumentException.class, () ->
                new TemplatedKafkaRatePromqlBuilder("  ")
        );
    }
}
