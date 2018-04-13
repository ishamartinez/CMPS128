package distributed;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import spark.Request;
import spark.Response;
import util.*;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class Server {

    public static final ObjectMapper mapper = new ObjectMapper();

    public final VersionVector versionVector;
    public final KeyValueStore keyStore;
    //public final Queue<String> gossipQueue;
    public final Set<String> replicaSet, failedSet, proxySet, deadSet;

    private final Map<Integer, Set<String>> partitionMap;

    private boolean isProxy;
    private String address, ip;
    private int K, partitionId, port;

    public Server() {

        versionVector = new VersionVector();
        keyStore = new KeyValueStore();

        //gossipQueue = new ConcurrentLinkedQueue<>();
        replicaSet = new ConcurrentSkipListSet<>();
        failedSet = new ConcurrentSkipListSet<>();
        proxySet = new ConcurrentSkipListSet<>();
        deadSet = new ConcurrentSkipListSet<>();

        partitionMap = Collections.synchronizedMap(new HashMap<>());

        isProxy = false;
        K = 1;
    }

    /****************************************************************
     *  Getters
     ****************************************************************/

    public String  getAddress()     { return address; }
    public String  getIp()          { return ip;      }
    public int     getPort()        { return port;    }
    public int     getK()           { return K;       }
    public boolean isProxy()        { return isProxy; }

    public int getPartitionCount() { return partitionMap.size(); }
    public int getPartitionId()    { return partitionId;         }

    public Set<String> getPartitionSet() {

        return getPartitionSet(partitionId);
    }

    public Set<String> getPartitionSet(int partitionId) {

        Set<String> partitionSet;
        partitionSet = partitionMap.get(partitionId);

        if(partitionSet == null)
            partitionSet = Collections.EMPTY_SET;

        return partitionSet;
    }

    public Set<Integer> getPartitionKeys() {

        return partitionMap.keySet();
    }

    /****************************************************************
     *  Setters
     ****************************************************************/

    public void setK(String setting) {

        if(setting != null)
            K = Math.max(K, Integer.parseInt(setting));
    }

    public void setK(int setting) {

        K = Math.max(K, setting);
    }

    public void setAddress(String setting) {

        address = setting;

        ip = address.split(":")[0];
        port = Integer.parseInt(address.split(":")[1]);

        replicaSet.add(address);
        versionVector.insert(address);
    }

    public void setView(String[] view) {

        versionVector.insertAll(view);
        replicaSet.addAll(Arrays.asList(view));

        partitionCheck();
    }

    /****************************************************************
     *  Set Helpers
     ****************************************************************/

    // Move address from replicaSet to failedSet
    public void replicaToFailed(String nodeId) {

        if(replicaSet.contains(nodeId) || proxySet.contains(nodeId)) {

            replicaSet.remove(nodeId);
            proxySet.remove(nodeId);
            failedSet.add(nodeId);

            broadcastFail(nodeId);
            partitionCheck();
        }
    }

    // Move address from failedSet to replicaSet
    public void failedToReplica(String nodeId) {

        if(failedSet.contains(nodeId)) {

            failedSet.remove(nodeId);
            replicaSet.add(nodeId);

            partitionCheck();
        }
    }

    // Move address from proxySet to replicaSet
    public void proxyToReplica(String nodeId) {

        if(proxySet.contains(nodeId)) {

            proxySet.remove(nodeId);
            replicaSet.add(nodeId);
        }
    }

    // Move address from replicaSet to proxySet
    public void replicaToProxy(String nodeId) {

        if(replicaSet.contains(nodeId)) {

            replicaSet.remove(nodeId);
            proxySet.add(nodeId);
        }
    }

    /****************************************************************
     *  Node add/remove helpers - details
     ****************************************************************/

    // Register node and send it view of the world
    public int addNode(String nodeId) {

        int partitionId;

        registerNode(nodeId);
        partitionId = inform(nodeId);

        return partitionId;
    }

    // Add a new node to the system
    public void registerNode(String nodeId) {

        if(!replicaSet.contains(nodeId) &&
                !proxySet.contains(nodeId) &&
                !failedSet.contains(nodeId)) {

            replicaSet.add(nodeId);
            versionVector.insert(nodeId);

            partitionCheck();
        }
    }

    // Remove a node from the system
    public void removeNode(String nodeId) {

        if(!deadSet.contains(nodeId)) {

            replicaSet.remove(nodeId);
            proxySet.remove(nodeId);
            failedSet.remove(nodeId);

            deadSet.add(nodeId);

            partitionCheck();
        }
    }

    // Send new node view of the world
    public int inform(String nodeId) {

        String url, body;
        HttpResponse<JsonNode> putResponse;
        JSONObject putBody;

        url = Urls.INFORM.build(nodeId);
        body =  "vector=" + versionVector.toString();
        body += "&replicaSet=" + mapper.stringify(replicaSet);
        body += "&failedSet=" + mapper.stringify(failedSet);
        body += "&proxySet=" + mapper.stringify(proxySet);
        body += "&deadSet=" + mapper.stringify(deadSet);

        putResponse = sendPutRequest(url, body, nodeId);

        if(putResponse != null) {

            putBody = putResponse.getBody().getObject();
            return putBody.getInt("partition_id");
        }

        return -1;
    }

    // Set of sorted 'live' nodes - replicas & proxies
    public SortedSet<String> liveSet() {

        SortedSet<String> liveSet;
        liveSet = new TreeSet<>();

        liveSet.addAll(replicaSet);
        liveSet.addAll(proxySet);

        return liveSet;
    }

    // Returns count of 'live' nodes - replicas & proxies
    public int liveCount() {

        int nodeCount;

        nodeCount = replicaSet.size();
        nodeCount += proxySet.size();

        return nodeCount;
    }

    /****************************************************************
     *  Misc Utility Functions
     ****************************************************************/

    // Special sync - syncs, adds difference and checks partition status
    public void syncVector(VersionVector otherVector) {

        Set<String> diff;
        diff = versionVector.syncDiff(otherVector);

        if(!diff.isEmpty()) {

            replicaSet.addAll(diff);

            partitionCheck();
        }
    }

    // Calculate partitions
    public void partitionCheck() {

        int partitionCount;
        Set<String> partitionSet;
        String[] liveSet;

        synchronized (partitionMap) {

            liveSet = liveSet().toArray(new String[0]);
            partitionCount = liveSet.length / K;

            // If no change in partition size return
            if(partitionCount == getPartitionCount()) return;

            partitionMap.clear();
            isProxy = false;

            // Populate partition map
            for(int i = 0; i < partitionCount; ++i) {

                partitionSet = new HashSet<>();

                for(int j = i * K; j < j + K; ++j) {

                    if(address.equals(liveSet[i]))
                        partitionId = i;

                    partitionSet.add(liveSet[j]);
                    proxyToReplica(liveSet[j]);
                }

                partitionMap.put(i, partitionSet);
            }

            // Remaining nodes are proxies
            for(int i = partitionCount * K; i < liveSet.length; ++i) {

                if(address.equals(liveSet[i])) {

                    isProxy = true;
                    partitionId = partitionCount;
                }

                replicaToProxy(liveSet[i]);
            }
        }
    }

	public void distributeKeys() {

		String[] liveSet;
		int liveCount;
		KeyValueStore notMyKeys = new KeyValueStore;
		liveSet = liveSet().toArray(new String[0]);
		liveCount = liveCount();
		
		// Iterate through all the keys in my keystore and broadcast any that do not belong
		Iterator<String> iterator;
        iterator = keyStore.keySet().iterator();
		
		while(iterator.hasNext()) {
			String key = iterator.next();
			String val = keyStore.get(key);
			
			if(key.hashCode()%P != partitionId){
				KeyValueStore.notMyKeys.put(key, val);
			}
		}
		for(int i = 0; i < liveCount-1; ++i) {
			//Juan-- Needs Url to be build that will send a keystore to a node which is liveSet[i]
			liveSet[i];
		}
	}

    /****************************************************************
     *  Request Functions
     ****************************************************************/

    // Send sync'd get request
    public HttpResponse<JsonNode> sendGetRequest(String url, String nodeId) {

        HttpResponse<JsonNode> jsonResponse = null;

        try {

            jsonResponse = Unirest.get(url).asJson();

        } catch (UnirestException e) {

            replicaToFailed(nodeId);
        }

        return jsonResponse;
    }

    // Send sync'd put request
    public HttpResponse<JsonNode> sendPutRequest(String url, String body, String nodeId) {

        HttpResponse<JsonNode> jsonResponse = null;

        try {

            jsonResponse = Unirest.put(url).body(body).asJson();

        } catch (UnirestException e) {

            replicaToFailed(nodeId);
        }

        return jsonResponse;
    }

    // Forwards a request to the first node within a partition that accepts it
    public JSONObject forwardRequest(Request request, Response response, String key) {

        int partitionId;
        Set<String> partitionSet;
        HttpResponse<JsonNode> forwardResponse = null;

        synchronized (partitionMap) {

            partitionId = key.hashCode() % getPartitionCount();
            partitionSet = getPartitionSet(partitionId);
        }

        for(String nodeId : partitionSet) {

            // Guard against self forward
            if(nodeId.equals(address)) continue;

            String url = Urls.GENERAL.build(nodeId, request.uri());
            if(request.queryString() != null)
                url = url + "?" + request.queryString();

            try {

                switch (request.requestMethod()) {

                    case "GET":

                        forwardResponse = Unirest.get(url).asJson();
                        break;

                    case "PUT":

                        forwardResponse = Unirest.put(url).body(request.body()).asJson();
                        break;

                    default:

                        response.status(Responses.INVALIDMETHOD.status);
                        return Responses.INVALIDMETHOD.json();
                }

                break;

            } catch (UnirestException e) {

                replicaToFailed(address);
            }
        }

        if(forwardResponse == null) {

            response.status(Responses.STOREDOWN.status);
            return Responses.STOREDOWN.json();
        }

        response.status(forwardResponse.getStatus());
        return forwardResponse.getBody().getObject();
    }

    /****************************************************************
     *  Broadcast Functions
     ****************************************************************/

    // Broadcast a put key too all live replicas
    public void broadcastKey(String key, Value value) {

        String body, jsonValue;
        Set<String> partitionSet;

        jsonValue = mapper.stringify(value);
        body = "val=" + jsonValue;

        partitionSet = getPartitionSet();
        broadcast(partitionSet, Urls.BROADCAST, key, body);
    }

    // Ping all dead nodes and send server address to identify self
    public void broadcastHeartbeat(String nodeId) {

        broadcast(failedSet, Urls.HEARTBEAT, nodeId);
    }

    // Broadcast addition or deletion of a node
    public void broadcastViewChange(String nodeId, String type) {

        String append;
        append = nodeId + "?type=" + type;

        broadcast(liveSet(), Urls.VIEWCAST, append);
    }

    // Broadcast the failure of a node to all live nodes
    public void broadcastFail(String nodeId) {
		
		broadcast(liveSet(), Urls.FAILCAST, nodeId);
    }

    // General broadcast function - always uses put - async
    private void broadcast(Set<String> ids, Urls builder, String append) {

        broadcast(ids, builder, append, "");
    }

    // General broadcast function - always uses put - async
    private void broadcast(Set<String> ids, Urls builder, String append, String body) {

        String url;

        for(String nodeId : ids) {

            // Guard against self broadcast
            if(nodeId.equals(address)) continue;

            url = builder.build(nodeId, append);
            Unirest.put(url).body(body).asJsonAsync(new Callback<JsonNode>() {

                @Override
                public void completed(HttpResponse<JsonNode> httpResponse) {

                    failedToReplica(nodeId);
                }

                @Override
                public void failed(UnirestException e) {

                    replicaToFailed(nodeId);
                }

                public void cancelled() {}
            });
        }
    }

    /****************************************************************
     *  Validation Functions
     ****************************************************************/

    // Validates key
    public static boolean validKey(String key) {

        return key.length() > 0 && key.length() < 201 && !key.matches(".*[^a-zA-Z0-9_].*");
    }

    // Validates Query
    public static boolean validQuery(String query) {

        return query != null && !query.equals("");
    }
}
