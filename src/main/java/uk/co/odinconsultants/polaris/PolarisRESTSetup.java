package uk.co.odinconsultants.polaris;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ClassicHttpResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;

import java.io.IOException;
import java.util.Map;

public class PolarisRESTSetup {
    private static final String clientId = "root";
    private static final String clientSecret = "secret";
    private static final String polarisUrl = "http://localhost:8181";
    public static final String accessToken = getAccessToken();
    public static final String WAREHOUSE_NAME = "manual_spark";

    public static void main(String[] args) {
        try {
            setup();
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    public static void setup() throws IOException {
        System.out.println("Starting Polaris REST API");
        postJsonCall("/api/management/v1/catalogs",
                "{\n" +
                "           \"catalog\": {\n" +
                        "             \"name\": \"" + WAREHOUSE_NAME + "\",\n" +
                "             \"type\": \"INTERNAL\",\n" +
                "             \"readOnly\": false,\n" +
                "             \"properties\": {\n" +
                "               \"default-base-location\": \"file:///tmp/polaris/\"\n" +
                "             },\n" +
                "             \"storageConfigInfo\": {\n" +
                "               \"storageType\": \"FILE\",\n" +
                "               \"allowedLocations\": [\n" +
                "                 \"file:///tmp/polaris/\"\n" +
                "               ]\n" +
                "             }\n" +
                "           }\n" +
                "         }");
        putJsonCall("/api/management/v1/catalogs/" + WAREHOUSE_NAME + "/catalog-roles/catalog_admin/grants",
                "{\"type\": \"catalog\", \"privilege\": \"TABLE_WRITE_DATA\"}");
        putJsonCall("/api/management/v1/principal-roles/service_admin/catalog-roles/" + WAREHOUSE_NAME,
                "{\"name\": \"catalog_admin\"}");
        System.out.println("Finished Polaris REST API");
    }

    private static void putJsonCall(String uri, String json) throws IOException {
        makeCallWithToken(json, new HttpPut(polarisUrl + uri));
    }

    private static void postJsonCall(String uri, String json) throws IOException {
        makeCallWithToken(json, new HttpPost(polarisUrl + uri));
    }

    private static void makeCallWithToken(String payload, BasicClassicHttpRequest post) throws IOException {
        post.setHeader("Authorization", "Bearer " + accessToken);
        post.setHeader("Accept", "application/json");
        post.setHeader("Content-Type", "application/json");
        makeCall(payload, post);
    }

    private static ClassicHttpResponse makeCall(String payload, BasicClassicHttpRequest post) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            post.setEntity(new StringEntity(payload));
            ClassicHttpResponse response = (ClassicHttpResponse) client.execute(post);
            assert response.getCode() == 200;
            return response;
        }
    }

    public static String getAccessToken() throws RuntimeException {
        HttpPost post = new HttpPost(polarisUrl + "/api/catalog/v1/oauth/tokens");
        post.setHeader("Content-Type", "application/x-www-form-urlencoded");
        String body = "grant_type=client_credentials&client_id=" + clientId +
                "&client_secret=" + clientSecret + "&scope=PRINCIPAL_ROLE:ALL";
        try {
            ClassicHttpResponse response = makeCall(body, post);
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> tokenResponse = mapper.readValue(response.getEntity().getContent(), Map.class);
            String accessTokenFromResponse = (String) tokenResponse.get("access_token");
            System.out.println("accessTokenFromResponse = " + accessTokenFromResponse);
            return accessTokenFromResponse;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
