package io.github.ruskaof.balancer.prometheus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TemplatedKafkaRatePromqlBuilderTest {

    @Test
    void replacesTopicsPlaceholder() {
        var b = new TemplatedKafkaRatePromqlBuilder(
                "sum(rate(my_bytes_total{topic=~\"%s\"}[1m])) by (topic, partition)");
        String q = b.setTopics(java.util.List.of("a", "b")).build();
        assertEquals(
                "sum(rate(my_bytes_total{topic=~\"a|b\"}[1m])) by (topic, partition)",
                q);
    }

    @Test
    void escapesDotInTopicForRegexLiteral() {
        var b = new TemplatedKafkaRatePromqlBuilder(
                "x{topic=~\"%s\"}y");
        String q = b.setTopics(java.util.List.of("foo.bar", "baz")).build();
        // The escape needs two backslashes in the query text: PromQL's string parser
        // consumes one level (and rejects a lone \. as an unknown escape sequence),
        // leaving \. for the regex engine.
        assertEquals("x{topic=~\"foo\\\\.bar|baz\"}y", q);
    }

    @Test
    void keepsLiteralPercentCharactersInTemplate() {
        var b = new TemplatedKafkaRatePromqlBuilder(
                "sum(rate(m{topic=~\"%s\",instance=~\"host-50%\"}[1m]))");
        String q = b.build(java.util.List.of("a"));
        assertEquals("sum(rate(m{topic=~\"a\",instance=~\"host-50%\"}[1m]))", q);
    }

    @Test
    void buildWithTopicsArgumentIsASingleStep() {
        var b = new TemplatedKafkaRatePromqlBuilder("x{topic=~\"%s\"}");
        assertEquals("x{topic=~\"a|b\"}", b.build(java.util.List.of("a", "b")));
    }

    @Test
    void rejectsMissingPlaceholder() {
        assertThrows(IllegalArgumentException.class, () -> new TemplatedKafkaRatePromqlBuilder("up"));
    }

    @Test
    void rejectsBlankTemplate() {
        assertThrows(IllegalArgumentException.class, () -> new TemplatedKafkaRatePromqlBuilder("  "));
    }
}
