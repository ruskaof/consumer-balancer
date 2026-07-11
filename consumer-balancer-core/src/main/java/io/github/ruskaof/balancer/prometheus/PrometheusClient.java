package io.github.ruskaof.balancer.prometheus;

import tools.jackson.databind.ObjectMapper;
import io.github.ruskaof.balancer.prometheus.model.PromqlResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
public class PrometheusClient implements AutoCloseable {

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
        URI uri = buildUri(settings, promql);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri)
                .GET()
                .timeout(settings.requestTimeout());
        if (settings.authorizationHeader() != null) {
            requestBuilder.header("Authorization", settings.authorizationHeader());
        }

        HttpResponse<byte[]> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        int code = response.statusCode();
        if (code < 200 || code >= 300) {
            String body = response.body() != null && response.body().length > 0
                    ? new String(response.body(), StandardCharsets.UTF_8)
                    : "";
            throw new IOException(
                    "Prometheus query failed: HTTP " + code + " body=" + truncate(body, 512) + " uri=" + uri);
        }

        PromqlResponse parsed = objectMapper.readValue(response.body(), PromqlResponse.class);
        if (!"success".equals(parsed.getStatus())) {
            throw new IOException("Prometheus query returned status '" + parsed.getStatus() + "'"
                    + (parsed.getErrorType() != null ? " errorType=" + parsed.getErrorType() : "")
                    + (parsed.getError() != null ? " error=" + parsed.getError() : "")
                    + " uri=" + uri);
        }
        return parsed;
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private static URI buildUri(PrometheusConnectionSettings settings, String promql) throws IOException {
        try {
            // URLEncoder encodes &, =, ? etc. as %26, %3D, %3F, preventing query parameter injection.
            // URI.create() treats the string as already raw/encoded and will not re-encode.
            String encodedPromql = URLEncoder.encode(promql, StandardCharsets.UTF_8);
            return URI.create(settings.baseUrl() + settings.pathPrefix()
                    + "/api/v1/query?query=" + encodedPromql);
        } catch (IllegalArgumentException e) {
            throw new IOException("Failed to build Prometheus URI", e);
        }
    }

    private static String truncate(String s, int max) {
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max) + "...";
    }
}
