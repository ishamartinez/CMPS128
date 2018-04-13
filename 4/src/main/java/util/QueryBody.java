package util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;

public class QueryBody {

    private HashMap<String, String> map;

    public QueryBody(String body) {

        map = new HashMap<>();

        try {
            body = URLDecoder.decode(body, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        parseBody(body);
    }

    private void parseBody(String body) {

        String[] queries = body.split("&");
        for(String query : queries) {

            String[] keyValue = query.split("=");

            if(keyValue.length  > 1)
                map.put(keyValue[0], keyValue[1]);
            else
                map.put(keyValue[0], "");
        }
    }

    public String get(String key) {

        return map.get(key);
    }
}
