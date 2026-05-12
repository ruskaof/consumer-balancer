package io.github.ruskaof.balancer.prometheus;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ruskaof.balancer.prometheus.model.PromqlResponse;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class PrometheusClientTest {

    @Test
    void getInstantValueParsesSuccessfulJson() throws Exception {
        String body = """
                {"status":"success","data":{"result":[{"metric":{"topic":"x","partition":"0"},"value":[0,"1.5"]}]}}
                """;

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/query", exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.start();
        try {
            int port = server.getAddress().getPort();
            PrometheusConnectionSettings settings = new PrometheusConnectionSettings(
                    "http",
                    "127.0.0.1",
                    port,
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(5));
            PrometheusClient client = new PrometheusClient(settings, new ObjectMapper());
            PromqlResponse r = client.getInstantValue("up");
            assertEquals("success", r.getStatus());
            assertNotNull(r.getData());
            assertEquals(1, r.getData().getResult().size());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void getInstantValueThrowsOnHttpError() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/query", exchange -> {
            exchange.sendResponseHeaders(500, 0);
            exchange.close();
        });
        server.start();
        try {
            int port = server.getAddress().getPort();
            PrometheusConnectionSettings settings = new PrometheusConnectionSettings(
                    "http",
                    "127.0.0.1",
                    port,
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(5));
            PrometheusClient client = new PrometheusClient(settings, new ObjectMapper());
            IOException ex = assertThrows(IOException.class, () -> client.getInstantValue("up"));
            assertTrue(ex.getMessage().contains("HTTP 500"));
        } finally {
            server.stop(0);
        }
    }
}
