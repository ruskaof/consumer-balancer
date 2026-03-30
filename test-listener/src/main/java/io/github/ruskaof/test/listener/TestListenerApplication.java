package io.github.ruskaof.test.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class TestListenerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestListenerApplication.class, args);
    }

    @KafkaListener(topics = "test-topic")
    public void consume(ConsumerRecord<String, byte[]> consumerRecord) {
    }
}
