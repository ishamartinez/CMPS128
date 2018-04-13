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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public class Server {

    public static final ObjectMapper mapper = new ObjectMapper();

    public static final VersionVector versionVector = new VersionVector();
    public static final KeyValueStore keyStore = new KeyValueStore();
    public static final ConcurrentLinkedQueue<String> priorityGossip = new ConcurrentLinkedQueue<>();
    public static final ConcurrentSkipListSet<String> liveList = new ConcurrentSkipListSet<>();
    public static final ConcurrentSkipListSet<String> failedList = new ConcurrentSkipListSet<>();
    public static final ConcurrentSkipListSet<String> proxyList = new ConcurrentSkipListSet<>();
    public static final ConcurrentSkipListSet<String> graveyard = new ConcurrentSkipListSet<>();

    private static boolean isProxy = false;
    private static String address, ip;
    private static int K = 1, port;

    public static String  getAddress() { return address; }
    public static String  getIp()      { return ip;      }
    public static int     getPort()    { return port;    }
    public static int     getK()       { return K;       }
    public static boolean isProxy()    { return isProxy; }

    public static void setK(String setting) {

        if(setting != null)
            K = Math.max(K, Integer.parseInt(setting));
    }

    public static void setK(int setting) {

            K = Math.max(K, setting);
    }

    public static void setAddress(String setting) {

        address = setting;

        ip = address.split(":")[0];
        port = Integer.parseInt(address.split(":")[1]);

        liveList.add(address);
        versionVector.insert(address);
    }

    // Moves an address from live to dead
    public static void liveToDead(String nodeId) {

        if(liveList.contains(nodeId) || proxyList.contains(nodeId)) {

            liveList.remove(nodeId);
            proxyList.remove(nodeId);
            failedList.add(nodeId);
        }
    }

    // Moves an address from dead to live and marks it as a gossip priority
    public static void deadToLive(String nodeId) {

        if(failedList.contains(nodeId)) {

            failedList.remove(nodeId);
            liveList.add(nodeId);

            // Add to priority gossip to catch up
            priorityGossip.add(nodeId);
        }
    }

    // Moves an address from proxy to live
    public static void proxyToLive(String nodeId) {

        if(proxyList.contains(nodeId)) {

            proxyList.remove(nodeId);
            liveList.add(nodeId);
        }
    }

    public static void liveToProxy(String nodeId) {

        if(liveList.contains(nodeId)) {

            liveList.remove(nodeId);
            proxyList.add(nodeId);
        }
    }

    // Add a new node - to live list and the version vector
    public static void addNode(String nodeId) {

        if(!liveList.contains(nodeId) &&
                !proxyList.contains(nodeId) &&
                !failedList.contains(nodeId)) {

            liveList.add(nodeId);
            versionVector.insert(nodeId);

            inform(nodeId);
        }
    }

    // Remove a node from every list and places it in graveyard
    public static void removeNode(String nodeId) {

        if(!graveyard.contains(nodeId)) {

            liveList.remove(nodeId);
            proxyList.remove(nodeId);
            failedList.remove(nodeId);

            graveyard.add(nodeId);
        }
    }

    // Generate a set with all potential gossip targets
    public static Set<String> gossipSet() {

        Set<String> gossipSet;
        gossipSet = new HashSet<>();

        gossipSet.addAll(liveList);
        gossipSet.addAll(proxyList);

        return gossipSet;
    }

    // Returns total count of nodes - not efficient but whatever
    public static int nodeCount() {

        Set<String> nodeSet;
        nodeSet = new HashSet<>();

        nodeSet.addAll(liveList);
        nodeSet.addAll(proxyList);

        return nodeSet.size();
    }

    // Special sync - syncs, adds difference and checks proxy status
    public static void syncVector(VersionVector otherVector) {

        Set<String> diff = Server.versionVector.syncDiff(otherVector);
        Server.liveList.addAll(diff);
        Server.priorityGossip.addAll(diff);
        Server.proxyCheck();
    }

    // Determines proxy status - called after view changes
    public static void proxyCheck() {

        String nodeId;
        String[] liveArray, proxyArray;
        int R, diff;

        liveArray = liveList.toArray(new String[0]);
        proxyArray = proxyList.toArray(new String[0]);

        R = liveArray.length;

        if(R > K && !isProxy) {

            diff = R - K;

            for(int i = R - diff; i < R; ++i) {

                nodeId = liveArray[i];
                if(address.equals(nodeId))
                    isProxy = true;

                liveToProxy(nodeId);
            }

        } else if(R < K && isProxy) {

            diff = K - R;

            for(int i = 0; i < proxyArray.length && i < diff; ++i) {

                nodeId = proxyArray[i];
                if(address.equals(nodeId))
                    isProxy = false;

                proxyToLive(address);
            }
        }
    }

    // New node - view change - send information to it
    public static void inform(String nodeId) {

        String url, body;

        url = Urls.INFORM.build(nodeId);
        body =  "vector=" + versionVector.toString();
        body += "&keystore=" + keyStore.toString();
        body += "&K=" + K;

        sendPutRequest(url, body, nodeId);
    }

    // Send single put sync request
    public static HttpResponse<JsonNode> sendGetRequest(String url, String nodeId) {

        HttpResponse<JsonNode> jsonResponse = null;

        try {

            jsonResponse = Unirest.get(url).asJson();

        } catch (UnirestException e) {

            Server.liveToDead(nodeId);
            Server.proxyCheck();
        }

        return jsonResponse;
    }

    // Send single put sync request
    public static HttpResponse<JsonNode> sendPutRequest(String url, String body, String nodeId) {

        HttpResponse<JsonNode> jsonResponse = null;

        try {

            jsonResponse = Unirest.put(url).body(body).asJson();

        } catch (UnirestException e) {

            Server.liveToDead(nodeId);
            Server.proxyCheck();
        }

        return jsonResponse;
    }

    // Forwards any request - at the moment it selects the first that accepts it's request
    public static JSONObject forwardRequest(Request request, Response response) {

        HttpResponse<JsonNode> forwardResponse = null;

        for(String nodeId : liveList) {

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

                liveToDead(address);
            }
        }

        proxyCheck();

        if(forwardResponse == null) {

            response.status(Responses.STOREDOWN.status);
            return Responses.STOREDOWN.json();
        }

        response.status(forwardResponse.getStatus());
        return forwardResponse.getBody().getObject();
    }

    // Broadcast a put key too all live replicas
    public static void broadcastKey(String key, Value value) {

        String body, jsonValue;

        jsonValue = mapper.stringify(value);
        body = "val=" + jsonValue;

        broadcast(liveList, Urls.BROADCAST, key, body);
    }

    // Ping all dead nodes and send server address to identify self
    public static void broadcastHeartbeat(String nodeId) {

        broadcast(failedList, Urls.HEARTBEAT, nodeId);
    }

    public static void broadcastViewChange(String nodeId, String type) {

        String append;
        Set<String> broadcastList;
        broadcastList = gossipSet();

        append = nodeId + "?type=" + type;

        broadcast(broadcastList, Urls.VIEWCAST, append);
    }

    // General broadcast function - always uses put - async
    private static void broadcast(Set<String> ids, Urls builder, String append) {

        broadcast(ids, builder, append, "");
    }

    // General broadcast function - always uses put - async
    private static void broadcast(Set<String> ids, Urls builder, String append, String body) {

        String url;

        for(String nodeId : ids) {

            // Guard against self broadcast
            if(nodeId.equals(address)) continue;

            url = builder.build(nodeId, append);
            Unirest.put(url).body(body).asJsonAsync(new Callback<JsonNode>() {

                @Override
                public void completed(HttpResponse<JsonNode> httpResponse) {

                    deadToLive(nodeId);
                }

                @Override
                public void failed(UnirestException e) {

                    liveToDead(nodeId);
                }

                public void cancelled() {}
            });
        }

        proxyCheck();
    }

    // Validates key
    public static boolean validKey(String key) {

        return key.length() > 0 && key.length() < 201 && !key.matches(".*[^a-zA-Z0-9_].*");
    }

    // Validates Query
    public static boolean validQuery(String query) {

        return query != null && !query.equals("");
    }
}
