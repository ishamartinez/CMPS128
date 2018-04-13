import static spark.Spark.*;

import com.mashape.unirest.http.*;
import distributed.GossipService;
import distributed.Server;
import distributed.VersionVector;
import org.json.JSONObject;
import util.*;

import java.util.Arrays;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) {

        initializeEnvironment();
        ignite();
    }

    private static void initializeEnvironment() {

        // Set server address
        Server.setAddress(System.getenv("IPPORT"));

        // Replica/Proxy Balance Constant
        Server.setK(System.getenv("K"));

        // Get initial view and build version vector and live list
        String view = System.getenv("VIEW");
        if (view != null) {

            String[] splitView = view.split(",");
            Server.versionVector.insertAll(splitView);
            Server.liveList.addAll(Arrays.asList(splitView));
        }

        // Determine proxy status
        Server.proxyCheck();

        // Start gossip service - 250ms intervals with 1000ms start delay
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new GossipService(), 1000, 200, TimeUnit.MILLISECONDS);

        // Set unirest timeouts for connection and socket
        Unirest.setTimeouts(1000, 4000);
    }

    private static void ignite() {

        ipAddress(Server.getIp());
        port(Server.getPort());

        JsonTransformer jsonTransformer;
        jsonTransformer = new JsonTransformer();

        // Default Response type is application/json
        before((request, response) -> response.type("application/json"));

        /*
            /kv-store/get_node_details GET route
            Return: result, replica
        */
        get("/kv-store/get_node_details", (request, response) -> {

            JSONObject jsonResponse;

            response.status(Responses.SUCCESS.status);
            jsonResponse = Responses.SUCCESS.json();

            if(Server.isProxy())
                jsonResponse.put("replica", "No");

            else
                jsonResponse.put("replica", "Yes");

            return jsonResponse;

        }, jsonTransformer);

        /*
            /kv-store/get_all_replicas/ GET route
            Return: result, replicas
        */
        get("/kv-store/get_all_replicas", (request, response) -> {

            JSONObject jsonResponse;

            response.status(Responses.SUCCESS.status);
            jsonResponse = Responses.SUCCESS.json();

            jsonResponse.put("replicas", Server.liveList);

            return jsonResponse;

        }, jsonTransformer);

        /*
            /kv-store/update_view PUT route
            Query Parameters: type, ip_port
            Return: result, msg, node_id, number_of_nodes
        */
        put("/kv-store/update_view", (request, response) -> {

            int nodeCount;
            String nodeId, type;
            JSONObject jsonResponse;

            response.status(Responses.SUCCESS.status);

            QueryBody qBody = new QueryBody(request.body());
            nodeId = qBody.get("ip_port");

            type = request.queryParams("type");            

            Server.broadcastViewChange(nodeId, type);

            if(type.equals("add")) {

                Server.addNode(nodeId);
                jsonResponse = Responses.SUCCESS2.json();
                jsonResponse.put("node_id", nodeId);

            } else {

                Server.removeNode(nodeId);
                jsonResponse = Responses.SUCCESS.json();
            }

            nodeCount = Server.nodeCount();
            jsonResponse.put("number_of_nodes", nodeCount);

            return jsonResponse;

        }, jsonTransformer);

        /*
           /kv-store/:key GET route
           Named Parameter: key
           Query Parameter: causal_payload
           Returns: result, value, node_id, causal_payload, timestamp
        */
        get("/kv-store/:key", (request, response) -> {

            String key, payload;
            JSONObject jsonResponse;

            key = request.params(":key");

            if(Server.isProxy())
                return Server.forwardRequest(request, response);

            if(!Server.validKey(key)) {

                response.status(Responses.INVALIDKEY.status);
                return Responses.INVALIDKEY.json();
            }

            if(Server.keyStore.containsKey(key)) {

                response.status(Responses.SUCCESS.status);
                jsonResponse = Responses.SUCCESS.json();

                Value storedValue = Server.keyStore.get(key);
                payload = Server.versionVector.incrementAndStringify(Server.getAddress());

                jsonResponse.put("value", storedValue.value);
                jsonResponse.put("node_id", Server.getAddress());
                jsonResponse.put("causal_payload", payload);
                jsonResponse.put("timestamp", storedValue.timeStamp);

            } else {

                response.status(Responses.NOKEY.status);
                jsonResponse = Responses.NOKEY.json();
            }

            return jsonResponse;

        }, jsonTransformer);

        /*
            /kv-store/:key PUT route
            Named Parameter: key
            Query Parameter: val, causal_payload
            Return: result, node_id, causal_payload, timestamp
        */
        put("/kv-store/:key", (request, response) -> {

            String key, val, payload;
            long timestamp;
            JSONObject jsonResponse;
            VersionVector clientVector;

            key = request.params(":key");

            QueryBody qBody = new QueryBody(request.body());
            val = qBody.get("val");
            payload = qBody.get("causal_payload");

            if(Server.isProxy())
                return Server.forwardRequest(request, response);

            if(!Server.validKey(key)) {

                response.status(Responses.INVALIDKEY.status);
                return Responses.INVALIDKEY.json();
            }

            if(!Server.validQuery(val)) {

                response.status(Responses.NOVALUE.status);
                return Responses.NOVALUE.json();
            }

            if(Server.keyStore.containsKey(key)) {

                response.status(Responses.SUCCESS.status);
                jsonResponse = Responses.SUCCESS.json();

            } else {

                response.status(Responses.CREATED.status);
                jsonResponse = Responses.CREATED.json();
            }

            if(Server.validQuery(payload)) {

                clientVector = Server.mapper.objectify(payload, VersionVector.class);

                // Sync vectors and merge
                Server.syncVector(clientVector);
                Server.versionVector.merge(clientVector);
            }

            payload = Server.versionVector.incrementAndStringify(Server.getAddress());
            timestamp = System.currentTimeMillis();

            Value newValue = new Value(val, payload, timestamp, Server.getAddress());

            Server.broadcastKey(key, newValue);
            Server.keyStore.put(key, newValue);

            jsonResponse.put("node_id", Server.getAddress());
            jsonResponse.put("causal_payload", payload);
            jsonResponse.put("timestamp", timestamp);

            return jsonResponse;
        
        }, jsonTransformer);

        /*
            /kv-store/gossip/:id GET route
            Named Parameter: id
            Return result, msg, vector, keystore, proxy, graveyard
        */
        get("/kv-store/gossip/:id", (request, response) -> {

            String id = request.params(":id");
            JSONObject jsonResponse = new JSONObject();

            Server.addNode(id);

            jsonResponse.put("vector", Server.versionVector.toString());
            jsonResponse.put("keystore", Server.keyStore.toString());
            jsonResponse.put("proxy", Server.isProxy());
            jsonResponse.put("graveyard", Server.graveyard);
            jsonResponse.put("K", Server.getK());

            return jsonResponse;

        }, jsonTransformer);

        /*
            /kv-store/broadcast/:key PUT route
            Named Parameter: key
            Query Parameter: val
            Return: result, msg
        */
        put("/kv-store/broadcast/:key", (request, response) -> {

            String key, val, id;
            Value receivedValue;
            VersionVector valueVector;

            key = request.params(":key");

            QueryBody qBody = new QueryBody(request.body());
            val = qBody.get("val");

            receivedValue = Server.mapper.objectify(val, Value.class);
            valueVector = Server.mapper.objectify(receivedValue.version, VersionVector.class);
            id = receivedValue.id;

            // Sync vectors and account for difference
            Server.syncVector(valueVector);
            Server.deadToLive(id);

            Server.keyStore.put(key, receivedValue);
            Server.versionVector.merge(valueVector);

            response.status(Responses.SUCCESS.status);
            return Responses.SUCCESS.json();

        }, jsonTransformer);

        /*
            /kv-store/heartbeat/:id PUT route
            Named Parameter: id
            Return: result, msg
        */
        put("/kv-store/heartbeat/:id", (request, response) -> {

            String id = request.params(":id");
            Server.deadToLive(id);

            response.status(Responses.SUCCESS.status);
            return Responses.SUCCESS.json();

        }, jsonTransformer);

        /*
            /kv-store/viewcast/:id PUT route
            Named Parameter: id
            Return: result, msg
        */
        put("/kv-store/viewcast/:id", (request, response) -> {

            String id, type;
            id = request.params(":id");
            type = request.queryParams("type");

            if(type.equals("add"))
                Server.addNode(id);

            else if (type.equals("remove"))
                Server.removeNode(id);

            response.status(Responses.SUCCESS.status);
            return Responses.SUCCESS.json();

        }, jsonTransformer);

         /*
            /kv-store/inform/ PUT route
            Return: result, msg
        */
        put("/kv-store/inform/", (request, response) -> {

            VersionVector otherVector;
            KeyValueStore otherStore;
            String K;

            QueryBody qBody = new QueryBody(request.body());
            otherVector = Server.mapper.objectify(qBody.get("vector"), VersionVector.class);
            otherStore  = Server.mapper.objectify(qBody.get("keystore"), KeyValueStore.class);
            K = qBody.get("K");

            Server.setK(K);

            Server.syncVector(otherVector);
            Server.keyStore.merge(otherStore);
            Server.versionVector.merge(otherVector);

            response.status(Responses.SUCCESS.status);
            return Responses.SUCCESS.json();

        }, jsonTransformer);
    }
}
