package com.flightanomaly.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;

public class OpenSkyClient {

    private final HttpClient httpClient;
    private final String clientId;
    private final String clientSecret;
    private String accessToken;
    private long tokenExpiresAt;  // epoch millis

    public OpenSkyClient(String clientId, String clientSecret) {
        this.httpClient = HttpClient.newHttpClient();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    public String fetchStates() throws Exception {
        String token = getToken();
        HttpRequest apiRequest = HttpRequest.newBuilder()
            .uri(URI.create("https://opensky-network.org/api/states/all"))
            .header("Authorization", "Bearer " + token)
            .GET()
            .build();
        HttpResponse<String> response = httpClient.send(apiRequest, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private String getToken() throws Exception {
        if (accessToken != null && System.currentTimeMillis() < tokenExpiresAt) {
            return accessToken;
        }

        HttpRequest tokenRequest = HttpRequest.newBuilder()
            .uri(URI.create("https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"))
            .header("Content-Type", "application/x-www-form-urlencoded")
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
}
