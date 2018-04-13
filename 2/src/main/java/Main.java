import static spark.Spark.*;

import com.google.gson.JsonParser;
import com.mashape.unirest.http.*;
import com.google.gson.JsonObject;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import spark.Response;

import java.util.concurrent.ConcurrentHashMap;

public class Main {

    private static ConcurrentHashMap<String, String> keyStore;

    private static boolean MAIN_INSTANCE;
    private static String MAINIP, IP;
    private static int PORT;

    public static void main(String[] args) {

        setEnvs();
        ignite();
    }

    private static void setEnvs() {

        // Yes Main IP - forwarding instance
        // No  Main IP - main instance
        MAINIP = System.getenv("MAINIP");

        if(MAINIP == null) {

            MAIN_INSTANCE = true;
            keyStore = new ConcurrentHashMap<>();

        } else {

            MAINIP = "http://" + MAINIP + "/kv-store/";
        }

        // IP Address
        IP = System.getenv("IP");
        IP = IP != null ? IP : "127.0.0.1";

        // Server Port
        String portStr = System.getenv("PORT");
        PORT = portStr != null ? Integer.parseInt(portStr) : 8080;
    }

    private static void ignite() {

        ipAddress(IP);
        port(PORT);

        JsonTransformer jsonTransformer;
        jsonTransformer = new JsonTransformer();

        // kv-store GET route
        // Parameters: key
        get("/kv-store/:key", (request, response) -> {

            String key = request.params(":key");

            response.type("application/json");

            // Validate key
            if(!validKey(key)) {

                response.status(GenericResponses.INVALIDKEY.status);
                return GenericResponses.INVALIDKEY.json;
            }

            if(!MAIN_INSTANCE)
                return forwardRequest("GET", key, null, response);

            JsonObject jObj = new JsonObject();

            if(keyStore.containsKey(key)) {

                response.status(200);
                jObj.addProperty("result", "Success");
                jObj.addProperty("value", keyStore.get(key));

            } else {

                response.status(404);
                jObj.addProperty("result", "Error");
                jObj.addProperty("msg", "Key does not exist");
            }

            return jObj;

        }, jsonTransformer);

        // kv-store PUT route
        // Parameters: key, val
        put("/kv-store/:key", (request, response) -> {

            String key = request.params(":key");
            String val = request.queryParams("val");

            response.type("application/json");

            // Validate key, value
            if(!validKey(key)) {

                response.status(GenericResponses.INVALIDKEY.status);
                return GenericResponses.INVALIDKEY.json;

            } else if(val == null) {

                response.status(GenericResponses.NOVALUE.status);
                return GenericResponses.NOVALUE.json;

            } else if(!validVal(val)) {

                response.status(GenericResponses.INVALIDVAL.status);
                return GenericResponses.INVALIDVAL.json;
            }

            if(!MAIN_INSTANCE)
                return forwardRequest("PUT", key, val, response);

            JsonObject jObj = new JsonObject();

            if(keyStore.containsKey(key)) {

                keyStore.put(key, val);
                response.status(200);
                jObj.addProperty("replaced", "True");
                jObj.addProperty("msg", "Value of existing key replaced");

            } else {

                keyStore.put(key, val);
                response.status(201);
                jObj.addProperty("replaced", "False");
                jObj.addProperty("msg", "New key created");
            }

            return jObj;
        
        }, jsonTransformer);

        // kv-store DEL route
        // Parameters: key
        delete("/kv-store/:key", (request, response) -> {

            String key = request.params(":key");

            response.type("application/json");

            // Validate key
            if(!validKey(key)) {

                response.status(GenericResponses.INVALIDKEY.status);
                return GenericResponses.INVALIDKEY.json;
            }

            if(!MAIN_INSTANCE)
                return forwardRequest("DELETE", key, null, response);

            JsonObject jObj = new JsonObject();

            if(keyStore.containsKey(key)){

                keyStore.remove(key);
                response.status(200);
                jObj.addProperty("result", "Success");

            } else {

                response.status(404);
                jObj.addProperty("result", "Error");
                jObj.addProperty("msg", "Key does not exist");
            }

            return jObj;

        }, jsonTransformer);
    }

    private static JsonObject forwardRequest(String method, String key, String val, Response response) {

        String url = MAINIP + key;
        HttpResponse<JsonNode> forwardRequest;

        try {

            if(method.equals("GET"))
                forwardRequest = Unirest.get(url).asJson();

            else if(method.equals("PUT"))
                forwardRequest = Unirest.put(url).queryString("val", val).asJson();

            else
                forwardRequest = Unirest.delete(url).asJson();

        } catch (UnirestException e) {

            response.status(GenericResponses.SERVICEDOWN.status);
            return GenericResponses.SERVICEDOWN.json;
        }

        response.status(forwardRequest.getStatus());

        // unirest json object to gson json object
        JSONObject forwardRequestBody = forwardRequest.getBody().getObject();
        JsonParser jsonParser = new JsonParser();

        return (JsonObject) jsonParser.parse(forwardRequestBody.toString());
    }

    private static boolean validKey(String key) {

        return key.length() > 0 && key.length() < 201 && !key.matches(".*[^a-zA-Z0-9_].*");
    }

    private static boolean validVal(String val) {

        return val.length() > 1 && val.length() <= 1000000;
    }
}