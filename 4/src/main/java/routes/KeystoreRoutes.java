package routes;

import distributed.Server;
import distributed.VersionVector;
import org.json.JSONObject;
import spark.Route;
import util.QueryBody;
import util.Responses;
import distributed.Value;

public class KeystoreRoutes {

    /*
        /kv-store/:key GET route
        Named Parameter: key
        Query Parameter: causal_payload
        Returns: result, value, partition_id, causal_payload, timestamp
    */
    public static Route getKey(Server server) {

        return (request, response) -> {

            int partitionId;
            String key, payload;
            JSONObject jsonResponse;

            key = request.params(":key");
            partitionId = key.hashCode() % server.getPartitionCount();

            if(server.isProxy() || partitionId != server.getPartitionId()) {

                return server.forwardRequest(request, response, key);

            } else if(!Server.validKey(key)) {

                response.status(Responses.INVALIDKEY.status);
                return Responses.INVALIDKEY.json();

            } else if(server.keyStore.containsKey(key)) {

                response.status(Responses.SUCCESS.status);
                jsonResponse = Responses.SUCCESS.json();

                Value storedValue = server.keyStore.get(key);
                payload = server.versionVector.incrementAndStringify(server.getAddress());

                jsonResponse.put("value", storedValue.value);
                jsonResponse.put("partition_id", server.getPartitionId());
                jsonResponse.put("causal_payload", payload);
                jsonResponse.put("timestamp", storedValue.timeStamp);

            } else {

                response.status(Responses.NOKEY.status);
                jsonResponse = Responses.NOKEY.json();
            }

            return jsonResponse;
        };
    }

    /*
        /kv-store/:key PUT route
        Named Parameter: key
        Query Parameter: val, causal_payload
        Return: result, partition_id, causal_payload, timestamp
    */
    public static Route putKey(Server server) {

        return (request, response) -> {

            int partitionId;
            long timestamp;
            String key, val, payload;
            JSONObject jsonResponse;
            VersionVector clientVector;

            key = request.params(":key");
            partitionId = key.hashCode() % server.getPartitionCount();

            QueryBody qBody = new QueryBody(request.body());
            val = qBody.get("val");
            payload = qBody.get("causal_payload");

            if(server.isProxy() || partitionId != server.getPartitionId()) {

                return server.forwardRequest(request, response, key);

            } else if(!Server.validKey(key)) {

                response.status(Responses.INVALIDKEY.status);
                return Responses.INVALIDKEY.json();

            } else if(!Server.validQuery(val)) {

                response.status(Responses.NOVALUE.status);
                return Responses.NOVALUE.json();

            } else if(server.keyStore.containsKey(key)) {

                response.status(Responses.SUCCESS.status);
                jsonResponse = Responses.SUCCESS.json();

            } else {

                response.status(Responses.CREATED.status);
                jsonResponse = Responses.CREATED.json();
            }

            if(Server.validQuery(payload)) {

                clientVector = Server.mapper.objectify(payload, VersionVector.class);

                // Sync vectors and merge
                server.syncVector(clientVector);
                server.versionVector.merge(clientVector);
            }

            payload = server.versionVector.incrementAndStringify(server.getAddress());
            timestamp = System.currentTimeMillis();

            Value newValue = new Value(val, payload, timestamp, server.getAddress());

            server.broadcastKey(key, newValue);
            server.keyStore.put(key, newValue);

            jsonResponse.put("partition_id", server.getPartitionId());
            jsonResponse.put("causal_payload", payload);
            jsonResponse.put("timestamp", timestamp);

            return jsonResponse;
        };
    }
}
