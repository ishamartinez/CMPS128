package distributed;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import org.json.JSONArray;
import org.json.JSONObject;
import util.Urls;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class GossipService implements Runnable {

    private Server server;

    public GossipService(Server server) {

        this.server = server;
    }

    @Override
    public void run() {

        if(!server.isProxy())
            partitionGossip();

        globalGossip();

        pollFailedNodes();
    }

    // Contact random partition node and exchange data
    private void partitionGossip() {

        String nodeId;
        String[] partitionSet;
        JSONObject jsonResponse;

        partitionSet = server.getPartitionSet().toArray(new String[0]);

        if(partitionSet.length > 1) {

            nodeId = pickNode(partitionSet);
            jsonResponse = gossip(nodeId);

            if (jsonResponse != null)
                partitionReconcile(nodeId, jsonResponse);
        }
    }

    // Contact random non-partition node and exchange data
    private void globalGossip() {

        String nodeId;
        Set<String> outsideSet;
        JSONObject jsonResponse;

        outsideSet = server.liveSet();
        outsideSet.removeAll(server.getPartitionSet());

        if(outsideSet.size() > 1) {

            nodeId = pickNode(outsideSet.toArray(new String[0]));
            jsonResponse = gossip(nodeId);

            if (jsonResponse != null)
                reconcile(nodeId, jsonResponse);
        }
    }

    private JSONObject gossip(String nodeId) {

        String url;
        HttpResponse<JsonNode> jsonResponse;

        url = Urls.GOSSIP.build(nodeId, server.getAddress());
        jsonResponse = server.sendGetRequest(url, nodeId);

        return jsonResponse.getBody().getObject();
    }

    // Select random node in given list
    private String pickNode(String[] nodeSet) {

        int randomIndex;
        String randomNode;

        do {

            randomIndex = ThreadLocalRandom.current().nextInt(0, nodeSet.length);

        } while (nodeSet[randomIndex].equals(server.getAddress()));

        randomNode = nodeSet[randomIndex];

        return randomNode;
    }

    // Merge the gossip data with server data
    private void reconcile(String nodeId, JSONObject receivedData) {

        boolean nodeIsProxy;
        VersionVector gossipVector;
        JSONArray deadSet;

        nodeIsProxy = receivedData.getBoolean("proxy");
        gossipVector = Server.mapper.objectify(receivedData.getString("vector"), VersionVector.class);

        deadSet = receivedData.getJSONArray("deadSet");


        // Check for dead nodes
        for(int i = 0; i < deadSet.length(); ++i) {

            String removedNode;
            removedNode = deadSet.getString(i);

            server.removeNode(removedNode);
        }

        // Check proxy status of gossiped node
        if(nodeIsProxy)
            server.replicaToProxy(nodeId);

        else
            server.proxyToReplica(nodeId);

        server.syncVector(gossipVector);
        server.versionVector.merge(gossipVector);
    }

    // TODO
    private void partitionReconcile(String nodeId, JSONObject receivedData) {

        KeyValueStore gossipStore;

        reconcile(nodeId, receivedData);

        gossipStore = Server.mapper.objectify(receivedData.getString("keystore"), KeyValueStore.class);
        server.keyStore.merge(gossipStore);
    }

    // Ping failed nodes to check if any are functioning again
    private void pollFailedNodes() {

        server.broadcastHeartbeat(server.getAddress());
    }
}
