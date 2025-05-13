package uk.co.odinconsultants.polaris;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ClassicHttpResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class PolarisRESTSetup {
    private static final String clientId = "root";
    private static final String clientSecret = "secret";
    private static final String polarisUrl = "http://localhost:8181";
    public static final String accessToken = getAccessToken();

    public static void main(String[] args) {
        try {
            setup();
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    public static void setup() throws IOException {
        postJsonCall("/api/management/v1/catalogs",
                "{\n" +
                "           \"catalog\": {\n" +
                "             \"name\": \"manual_spark\",\n" +
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
        putJsonCall("/api/management/v1/catalogs/manual_spark/catalog-roles/catalog_admin/grants",
                "{\"type\": \"catalog\", \"privilege\": \"TABLE_WRITE_DATA\"}");
        putJsonCall("/api/management/v1/principal-roles/service_admin/catalog-roles/manual_spark",
                "{\"name\": \"catalog_admin\"}");
    }

    private static void putJsonCall(String uri, String json) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            var post = new HttpPut(polarisUrl + uri);
            post.setHeader("Authorization", "Bearer " + accessToken);
            post.setHeader("Accept", "application/json");
            post.setHeader("Content-Type", "application/json");
            post.setEntity(new StringEntity(json));

            ClassicHttpResponse response = (ClassicHttpResponse) client.execute(post);
            assert response.getCode() == 200;
        }
    }

    private static void postJsonCall(String uri, String json) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(polarisUrl + uri);
            post.setHeader("Authorization", "Bearer " + accessToken);
            post.setHeader("Accept", "application/json");
            post.setHeader("Content-Type", "application/json");
            post.setEntity(new StringEntity(json));

            ClassicHttpResponse response = (ClassicHttpResponse) client.execute(post);
            assert response.getCode() == 200;
        }
    }

    public static String getAccessToken() throws RuntimeException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(polarisUrl + "/api/catalog/v1/oauth/tokens");
            post.setHeader("Content-Type", "application/x-www-form-urlencoded");
            String body = "grant_type=client_credentials&client_id=" + clientId +
                    "&client_secret=" + clientSecret + "&scope=PRINCIPAL_ROLE:ALL";
            post.setEntity(new StringEntity(body));

            ClassicHttpResponse response = (ClassicHttpResponse) client.execute(post);
            assert response.getCode() == 200;
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> tokenResponse = mapper.readValue(response.getEntity().getContent(), Map.class);
            String accessToken = (String) tokenResponse.get("access_token");
            System.out.println("********** Access Token: " + accessToken);
            return accessToken;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
