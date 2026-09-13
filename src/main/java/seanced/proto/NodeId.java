package seanced.proto;

import java.util.Objects;

/**
 * A node's network identity: the address its peers reach it on.
 *
 * <p>Used as a map key throughout the membership table, so value equality (free
 * from the record) is load-bearing.
 */
public record NodeId(String host, int port) {

    public NodeId {
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
    }

    /** Parses {@code host:port}. */
    public static NodeId parse(String text) {
        Objects.requireNonNull(text, "text");
        int colon = text.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("expected host:port, got: " + text);
        }
        String host = text.substring(0, colon);
        int port;
        try {
            port = Integer.parseInt(text.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad port in: " + text, e);
        }
        return new NodeId(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
