package routes;

import distributed.Server;
import distributed.VersionVector;
import spark.Route;
import util.QueryBody;
import util.Responses;
import distributed.Value;

public class BroadcastRoutes {

    /*
        /kv-store/heartbeat/:id PUT route
        Named Parameter: id
        Return: result, msg
    */
    public static Route heartbeat(Server server) {

        return (request, response) -> {

            String id = request.params(":id");
            server.failedToReplica(id);

            response.status(Responses.SUCCESS.status);
            return Responses.SUCCESS.json();
        };
    }

    /*
        /kv-store/broadcast/:key PUT route
        Named Parameter: key
        Query Parameter: val
        Return: result, msg
    */
    public static Route keycast(Server server) {

        return (request, response) -> {

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
            server.syncVector(valueVector);
            server.failedToReplica(id);

			if((key.hashCode() % server.getPartitionCount()) == server.getPartitionId())
				server.keyStore.put(key, receivedValue);
			else
			    server.forwardRequest(request, response, key);

            server.versionVector.merge(valueVector);
			
			response.status(Responses.SUCCESS.status);
			return Responses.SUCCESS.json();
            
        };
    }

    /*
        /kv-store/viewcast/:id PUT route
        Named Parameter: id
        Return: result, msg
    */
    public static Route viewcast(Server server) {

        return (request, response) -> {

            String id, type;
            id = request.params(":id");
            type = request.queryParams("type");

            if(type.equals("add"))
                server.registerNode(id);

            else if (type.equals("remove"))
                server.removeNode(id);

            response.status(Responses.SUCCESS.status);
            return Responses.SUCCESS.json();
        };
    }

    /*
        /kv-store/failcast/:id PUT route
        Return: result, msg
    */
    public static Route failcast(Server server) {

        return (request, response) -> {

			 String id;
			 id = request.params(":id");

			 server.replicaToFailed(id);

			 response.status(Responses.SUCCESS.status);
			 return Responses.SUCCESS.json();
        };
    }
}
