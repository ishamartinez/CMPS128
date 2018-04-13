package routes;

import distributed.Server;
import org.json.JSONObject;
import spark.Route;
import util.QueryBody;
import util.Responses;

import java.util.Set;

public class DetailRoutes {

    /*
        /kv-store/get_all_replicas/ GET route
        Return: result, replicas
    */
    public static Route allReplicas(Server server) {

        return (request, response) -> {

            JSONObject jsonResponse;

            response.status(Responses.SUCCESS.status);
            jsonResponse = Responses.SUCCESS.json();

            jsonResponse.put("replicas", server.replicaSet);

            return jsonResponse;
        };
    }

    /*
        /kv-store/get_node_details GET route
        Return: result, replica
    */
    public static Route nodeDetails(Server server) {

        return (request, response) -> {

            JSONObject jsonResponse;

            response.status(Responses.SUCCESS.status);
            jsonResponse = Responses.SUCCESS.json();

            if(server.isProxy())
                jsonResponse.put("replica", "No");

            else
                jsonResponse.put("replica", "Yes");

            return jsonResponse;
        };
    }

    /*
        /kv-store/gossip/:id GET route
        Named Parameter: id
        Return result, msg, vector, keystore, proxy, deadSet
    */
    public static Route gossip(Server server) {

        return (request, response) -> {

            String id = request.params(":id");
            JSONObject jsonResponse = new JSONObject();

            server.registerNode(id);

            jsonResponse.put("vector", server.versionVector.toString());
            jsonResponse.put("keystore", server.keyStore.toString());
            jsonResponse.put("proxy", server.isProxy());
            jsonResponse.put("deadSet", server.deadSet);

            return jsonResponse;
        };
    }

    /*
        /kv-store/get_partition_id GET route
        Return: result, partition_id
    */
    public static Route partitionId(Server server) {

        return (request, response) -> {

            response.status(Responses.SUCCESS.status);

            JSONObject jsonResponse = Responses.SUCCESS.json();
            jsonResponse.put("partition_id", server.getPartitionId());

            return jsonResponse;
        };
    }

    /*
        /kv-store/get_all_partition_ids GET route
        Return: result, partition_id_list
    */
    public static Route partitionIds(Server server) {

        return (request, response) -> {

            JSONObject jsonResponse;

            response.status(Responses.SUCCESS.status);
            jsonResponse = Responses.SUCCESS.json();
            jsonResponse.put("partition_id_list", server.getPartitionKeys());

            return jsonResponse;
        };
    }

    /*
        /kv-store/get_partition_members GET route
        Return: result, partition_members
    */
    public static Route partitionMembers(Server server) {

        return (request, response) -> {

            int partitionId;
            Set<String> members;
            JSONObject jsonResponse;

            QueryBody qBody = new QueryBody(request.body());
            partitionId = Integer.parseInt(qBody.get("partition_id"));

            members = server.getPartitionSet(partitionId);

            if(members.isEmpty()) {

                response.status(Responses.NOPARTITIONID.status);
                return Responses.NOPARTITIONID.json();
            }

            response.status(Responses.SUCCESS.status);
            jsonResponse = Responses.SUCCESS.json();
            jsonResponse.put("partition_members", members);

            return jsonResponse;
        };
    }
}
