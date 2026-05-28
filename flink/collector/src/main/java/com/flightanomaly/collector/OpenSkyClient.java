package com.flightanomaly.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class OpenSkyClient {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSkyClient.class);

    private final HttpClient httpClient;
    private final String clientId;
    private final String clientSecret;
    private String accessToken;
    private long tokenExpiresAt;

    private long retryAfterUntil;

    public OpenSkyClient(String clientId, String clientSecret) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    public OptionalLong getRetryWaitMs() {
        long wait = retryAfterUntil - System.currentTimeMillis();
        return wait > 0 ? OptionalLong.of(wait) : OptionalLong.empty();
    }

    public String fetchStates() throws Exception {
        OptionalLong wait = getRetryWaitMs();
        if (wait.isPresent()) {
            throw new RateLimitException(wait.getAsLong());
        }

        String token = getToken();

        String bbox = System.getenv("OPENSKY_BBOX");
        String url = "https://opensky-network.org/api/states/all";
        if (bbox != null && !bbox.isBlank()) {
            String[] parts = bbox.split(",");
            url += "?lamin=" + parts[0] + "&lomin=" + parts[1]
                 + "&lamax=" + parts[2] + "&lomax=" + parts[3];
        }

        HttpRequest apiRequest = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + token)
            .timeout(Duration.ofSeconds(120))
            .GET()
            .build();
        HttpResponse<String> response = httpClient.send(apiRequest, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 429) {
            long retryAfterSec = response.headers()
                    .firstValueAsLong("X-Rate-Limit-Retry-After-Seconds")
                    .orElse(300);
            retryAfterUntil = System.currentTimeMillis() + retryAfterSec * 1000;
            throw new RateLimitException(retryAfterSec * 1000);
        }

        if (response.statusCode() != 200) {
            throw new RuntimeException("OpenSky API returned HTTP " + response.statusCode() + ": " + response.body());
        }

        response.headers().firstValueAsLong("X-Rate-Limit-Remaining").ifPresent(remaining ->
                LOG.info("OpenSky credits remaining: {}", remaining));

        return response.body();
    }

    private String getToken() throws Exception {
        if (accessToken != null && System.currentTimeMillis() < tokenExpiresAt) {
            return accessToken;
        }

        HttpRequest tokenRequest = HttpRequest.newBuilder()
            .uri(URI.create("https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(
                "grant_type=client_credentials&client_id=" + clientId + "&client_secret=" + clientSecret))
            .build();

        HttpResponse<String> response = httpClient.send(tokenRequest, HttpResponse.BodyHandlers.ofString());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response.body());
        this.accessToken = json.get("access_token").asText();
        long expiresIn = json.get("expires_in").asLong();
        this.tokenExpiresAt = System.currentTimeMillis() + (expiresIn * 1000) - 30000;
        return this.accessToken;
    }

    public static class RateLimitException extends Exception {
        private final long waitMs;

        public RateLimitException(long waitMs) {
            super("Rate limited — retry after " + (waitMs / 1000) + "s");
            this.waitMs = waitMs;
        }

        public long getWaitMs() {
            return waitMs;
        }
    }
}
