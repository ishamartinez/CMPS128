import com.google.gson.JsonObject;

public enum GenericResponses {

    INVALIDKEY  (403, "Error", "Key not valid"),
    INVALIDVAL  (403, "Error", "Object too large. Size limit is 1MB"),
    NOVALUE     (403, "Error", "No value provided"),
    SERVICEDOWN (500, "Error", "Server unavailable");

    JsonObject json;
    int status;

    GenericResponses(int status, String result, String msg) {

        this.status = status;

        json = new JsonObject();
        json.addProperty("result", result);
        json.addProperty("msg", msg);
    }
}
