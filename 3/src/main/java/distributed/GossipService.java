package distributed;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONArray;
import org.json.JSONObject;
import util.KeyValueStore;
import util.Urls;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class GossipService implements Runnable {

    private String[] gossipList;

    @Override
    public void run() {

        Set<String> gossipSet;
        gossipSet = Server.gossipSet();

        gossipList = gossipSet.toArray(new String[0]);

        if(canGossip())
            gossip();

        pollDead();
    }

    // Check if there are node to whom we can gossip
    private boolean canGossip() {

        return gossipList.length > 1 || !Server.priorityGossip.isEmpty();
    }

    // Contact priority/random node and compare data
    private void gossip() {

        String nodeId, url;
        HttpResponse<JsonNode> jsonResponse;

        nodeId = pickNode();
        url = Urls.GOSSIP.build(nodeId, Server.getAddress());
        jsonResponse = Server.sendGetRequest(url, nodeId);

        if(jsonResponse != null)
            reconcile(nodeId, jsonResponse.getBody().getObject());
    }

    // Either a priority gossip target is chosen or a random live node
    private String pickNode() {

        String nodeId;
        nodeId = Server.priorityGossip.poll();

        if(nodeId == null) {

            int index = ThreadLocalRandom.current().nextInt(0, gossipList.length);

            while (gossipList[index].equals(Server.getAddress())) {

                index = ThreadLocalRandom.current().nextInt(0, gossipList.length);
            }

            nodeId = gossipList[index];
        }

        return nodeId;
    }

    // Merge the fucking gossip data with your own
    private void reconcile(String nodeId, JSONObject receivedData) {

        int K;
        boolean nodeIsProxy;
        VersionVector gossipVector;
        KeyValueStore gossipStore;
        JSONArray graveyard;

        nodeIsProxy = receivedData.getBoolean("proxy");
        gossipVector = Server.mapper.objectify(receivedData.getString("vector"), VersionVector.class);
        gossipStore = Server.mapper.objectify(receivedData.getString("keystore"), KeyValueStore.class);
        graveyard = receivedData.getJSONArray("graveyard");
        K = receivedData.getInt("K");

        Server.setK(K);

        // Check for graveyard nodes
        for(int i = 0; i < graveyard.length(); ++i) {

            String removedNode;
            removedNode = graveyard.getString(i);

            Server.removeNode(removedNode);
        }

        // Check proxy status of gossiped node
        if(nodeIsProxy)
            Server.liveToProxy(nodeId);

        else
            Server.proxyToLive(nodeId);

        // Sync, kv merge, merge
        Server.syncVector(gossipVector);

        if(!Server.isProxy())
            Server.keyStore.merge(gossipStore);

        Server.versionVector.merge(gossipVector);
    }

    // Ping dead nodes to check if any have been resurrected
    // Every parting gives a foretaste of death, every reunion a hint of the resurrection.
    // - Arthur Schopenhauer
    private void pollDead() {

        Server.broadcastHeartbeat(Server.getAddress());
    }
}
