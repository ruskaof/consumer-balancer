package com.ruskaof.balancer.prometheus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ruskaof.balancer.prometheus.model.PromqlResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
public class PrometheusClient {

    private final HttpClient httpClient;
    private final PrometheusConnectionSettings settings;
    private final ObjectMapper objectMapper;

    public PrometheusClient(PrometheusConnectionSettings settings, ObjectMapper objectMapper) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(settings.connectTimeout())
                .build();
    }

    public PromqlResponse getInstantValue(String promql) throws IOException, InterruptedException {
        Objects.requireNonNull(promql, "promql");
        URI uri = UriComponentsBuilder.newInstance()
                .scheme(settings.scheme())
                .host(settings.host())
                .port(settings.port())
                .path("/api/v1/query")
                .queryParam("query", promql)
                .build()
                .encode(StandardCharsets.UTF_8)
                .toUri();

        HttpRequest request = HttpRequest.newBuilder(uri)
                .GET()
                .timeout(settings.requestTimeout())
                .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        int code = response.statusCode();
        if (code < 200 || code >= 300) {
            String body = response.body() != null && response.body().length > 0
                    ? new String(response.body(), StandardCharsets.UTF_8)
                    : "";
            throw new IOException(
                    "Prometheus query failed: HTTP " + code + " body=" + truncate(body, 512) + " uri=" + uri);
        }

        return objectMapper.readValue(response.body(), PromqlResponse.class);
    }

    private static String truncate(String s, int max) {
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max) + "...";
    }
}
