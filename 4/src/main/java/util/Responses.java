package util;

import org.json.JSONObject;

public enum Responses {

    SUCCESS       (200, "success", "OK"),
    SUCCESS2      (200, "success", "success"),
    CREATED       (201, "success", "Created"),
    INVALIDKEY    (403, "error",   "Key not valid"),
    NOVALUE       (403, "error",   "No value provided"),
    NOKEY         (404, "error",   "Key not found"),
    NOPARTITIONID (404, "error",   "Partition id not found"),
    INVALIDMETHOD (405, "error",   "Invalid method"),
    SERVICEDOWN   (500, "error",   "Server unavailable"),
    STOREDOWN     (500, "error",   "KeyStore unavailable");

    public final int status;
    private String result, message;

    Responses(int status, String result, String message) {

        this.status = status;
        this.result = result;
        this.message = message;
    }

    public JSONObject json() {

        JSONObject json = new JSONObject();
        json.put("result", result);
        json.put("msg", message);

        return json;
    }
}
