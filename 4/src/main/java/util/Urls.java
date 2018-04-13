package util;

public enum Urls {

    GENERAL   (""),
    KEYSTORE  ("/kv-store/"),
    GOSSIP    ("/kv-store/gossip/"),
    BROADCAST ("/kv-store/keycast/"),
    HEARTBEAT ("/kv-store/heartbeat/"),
    VIEWCAST  ("/kv-store/viewcast/"),
    INFORM    ("/kv-store/inform/"),
    FAILCAST  ("/kv-store/failcast/");

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
