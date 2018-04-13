import static spark.Spark.*;

import com.mashape.unirest.http.*;
import distributed.GossipService;
import distributed.Server;
import routes.*;
import util.*;

import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) {

        Server server = new Server();

        initializeEnvironment(server);
        ignite(server);
    }

    private static void initializeEnvironment(Server server) {

        // Set server address
        server.setAddress(System.getenv("IPPORT"));

        // Replica/Proxy Balance Constant
        server.setK(System.getenv("K"));

        // Get initial view and build version vector and live list
        String view = System.getenv("VIEW");
        if (view != null)
            server.setView(view.split(","));

        // Start gossip service - 250ms intervals with 1000ms start delay
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new GossipService(server), 1000, 100, TimeUnit.MILLISECONDS);

        // Set unirest timeouts for connection and socket
        Unirest.setTimeouts(1000, 4000);
    }

    private static void ignite(Server server) {

        ipAddress(server.getIp());
        port(server.getPort());

        JsonTransformer jsonTransformer;
        jsonTransformer = new JsonTransformer();

        // Default Response type is application/json
        before((request, response) -> response.type("application/json"));

        /* **************************************************************
         *  Detail Routes
         ****************************************************************/

        get("/kv-store/get_node_details",
                DetailRoutes.nodeDetails(server), jsonTransformer);

        get("/kv-store/get_all_replicas",
                DetailRoutes.allReplicas(server), jsonTransformer);

        get("/kv-store/gossip/:id",
                DetailRoutes.gossip(server), jsonTransformer);

        get("/kv-store/get_partition_id",
                DetailRoutes.partitionId(server), jsonTransformer);

        get("/kv-store/get_all_partition_ids",
                DetailRoutes.partitionIds(server), jsonTransformer);

        get("/kv-store/get_partition_members",
                DetailRoutes.partitionMembers(server), jsonTransformer);

        /* **************************************************************
         *  Update Routes
         ****************************************************************/

        put("/kv-store/update_view",
                UpdateRoutes.update(server), jsonTransformer);

        put("/kv-store/inform/",
                UpdateRoutes.inform(server), jsonTransformer);

        /* **************************************************************
         *  KeyValueStore Routes
         ****************************************************************/

        get("/kv-store/:key",
                KeystoreRoutes.getKey(server), jsonTransformer);

        put("/kv-store/:key",
                KeystoreRoutes.putKey(server), jsonTransformer);

        /* **************************************************************
         *  Broadcast Routes
         ****************************************************************/

        put("/kv-store/keycast/:key",
                BroadcastRoutes.keycast(server), jsonTransformer);

        put("/kv-store/heartbeat/:id",
                BroadcastRoutes.heartbeat(server), jsonTransformer);

        put("/kv-store/viewcast/:id",
                BroadcastRoutes.viewcast(server), jsonTransformer);

        put("/kv-store/failcast/:id",
                BroadcastRoutes.failcast(server), jsonTransformer);
    }
}
