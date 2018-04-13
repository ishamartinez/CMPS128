package routes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.org.apache.regexp.internal.RE;
import distributed.Server;
import distributed.VersionVector;
import org.json.JSONObject;
import spark.Route;
import util.QueryBody;
import util.Responses;

import java.util.Set;

public class UpdateRoutes {

    /*
        /kv-store/inform/ PUT route
        Return: result, msg
    */
    public static Route inform(Server server) {

        return (request, response) -> {

            VersionVector otherVector;
            TypeReference<Set<String>> typeReference;
            Set<String> replicaSet, failedSet, proxySet, deadSet;
            JSONObject jsonResponse;

            QueryBody qBody = new QueryBody(request.body());

            // Rebuild passed vector
            otherVector = Server.mapper.objectify(qBody.get("vector"), VersionVector.class);

            // Rebuild passed sets
            typeReference = new TypeReference<Set<String>>() {};
            replicaSet = Server.mapper.objectify(qBody.get("replicaSet"), typeReference);
            failedSet = Server.mapper.objectify(qBody.get("failedSet"), typeReference);
            proxySet = Server.mapper.objectify(qBody.get("proxySet"), typeReference);
            deadSet = Server.mapper.objectify(qBody.get("deadSet"), typeReference);

            server.replicaSet.addAll(replicaSet);
            server.failedSet.addAll(failedSet);
            server.proxySet.addAll(proxySet);
            server.deadSet.addAll(deadSet);

            server.versionVector.sync(otherVector);
            server.versionVector.merge(otherVector);

            server.partitionCheck();

            response.status(Responses.SUCCESS.status);

            jsonResponse = Responses.SUCCESS.json();
            jsonResponse.put("partition_id", server.getPartitionId());

            return Responses.SUCCESS.json();
        };
    }

    /*
        /kv-store/update_view PUT route
        Query Parameters: type, ip_port
        Return: result, partition_id, number_of_partitions
    */
    public static Route update(Server server) {

        return (request, response) -> {

            int partitionId, partitionCount;
            String nodeId, type;
            JSONObject jsonResponse;

            QueryBody qBody = new QueryBody(request.body());
            nodeId = qBody.get("ip_port");

            type = request.queryParams("type");
            server.broadcastViewChange(nodeId, type);

            jsonResponse = Responses.SUCCESS.json();

            if(type.equals("add")) {

                partitionId = server.addNode(nodeId);
                if(partitionId == -1) {

                    response.status(Responses.STOREDOWN.status);
                    return Responses.STOREDOWN.json();
                }

                jsonResponse.put("partition_id", partitionId);

            } else {

                server.removeNode(nodeId);
            }

            partitionCount = server.getPartitionCount();
            jsonResponse.put("number_of_partitions", partitionCount);

            response.status(Responses.SUCCESS.status);
            return jsonResponse;
        };
    }
}
