package seanced.proto;

import java.util.List;
import java.util.Objects;

/**
 * A direct probe: "are you there?"
 *
 * <p>Sent once per protocol period to one randomly chosen peer. The reply is an
 * {@link Ack} carrying the same {@code seqNo}.
 */
public record Ping(NodeId from, long seqNo, List<MembershipUpdate> gossip) implements Message {

    public Ping {
        Objects.requireNonNull(from, "from");
        gossip = List.copyOf(gossip);
    }

    public Ping(NodeId from, long seqNo) {
        this(from, seqNo, List.of());
    }
}
