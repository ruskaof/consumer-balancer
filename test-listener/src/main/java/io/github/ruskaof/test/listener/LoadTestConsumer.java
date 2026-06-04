package io.github.ruskaof.test.listener;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

/**
 * Test consumer used by the performance harness.
 *
 * <p>Two behaviours make the benefit of load-aware assignment observable:
 * <ul>
 *   <li>A fixed, configurable <em>service time</em> per record. With a fixed
 *       cost per message, a consumer thread can process at most
 *       {@code 1 / serviceTime} messages per second. A consumer that is assigned
 *       too much partition load therefore falls behind, producing measurable
 *       consumer lag. We use {@link LockSupport#parkNanos(long)} (sleep, not a
 *       busy-spin) so the service time models I/O-bound processing without
 *       burning CPU and skewing the co-located consumer groups against each
 *       other.</li>
 *   <li>An end-to-end latency timer. The producer embeds its wall-clock send
 *       time (epoch millis) in the payload; we record {@code now - sentAt} into a
 *       Micrometer timer with a percentile histogram, so the harness can query
 *       p99 latency per consumer group via {@code histogram_quantile(...)}.</li>
 * </ul>
 */
@Component
public class LoadTestConsumer {

    private final long serviceTimeNanos;
    private final Timer e2eLatency;

    public LoadTestConsumer(
            MeterRegistry meterRegistry,
            @Value("${listener.processing-cost-micros:0}") long processingCostMicros) {
        this.serviceTimeNanos = Math.max(0L, processingCostMicros) * 1_000L;
        this.e2eLatency = Timer.builder("e2e_latency")
                .description("End-to-end latency from produce to consume")
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    @KafkaListener(topics = "test-topic")
    public void consume(ConsumerRecord<String, byte[]> consumerRecord) {
        if (serviceTimeNanos > 0L) {
            LockSupport.parkNanos(serviceTimeNanos);
        }
        recordEndToEndLatency(consumerRecord);
    }

    private void recordEndToEndLatency(ConsumerRecord<String, byte[]> consumerRecord) {
        byte[] value = consumerRecord.value();
        if (value == null || value.length == 0) {
            return;
        }
        try {
            long producedAtMillis = Long.parseLong(new String(value, StandardCharsets.UTF_8).trim());
            long latencyMillis = System.currentTimeMillis() - producedAtMillis;
            if (latencyMillis >= 0L) {
                e2eLatency.record(Duration.ofMillis(latencyMillis));
            }
        } catch (NumberFormatException ignored) {
            // Payload is not a timestamp (e.g. legacy/manual producers): skip latency accounting.
        }
    }
}
