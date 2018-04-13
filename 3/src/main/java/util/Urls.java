package util;

public enum Urls {

    GENERAL   (""),
    KEYSTORE  ("/kv-store/"),
    GOSSIP    ("/kv-store/gossip/"),
    BROADCAST ("/kv-store/broadcast/"),
    HEARTBEAT ("/kv-store/heartbeat/"),
    VIEWCAST  ("/kv-store/viewcast/"),
    //PROXYCAST ("/kv-store/proxycast/"),
    INFORM    ("/kv-store/inform/");

    private static final String protocol = "http://";
    private String route;

    Urls(String route) {

        this.route = route;
    }

    public String build(String address) {

        return protocol + address + route;
    }

    public String build(String address, String append) {

        return protocol + address + route + append;
    }
}
