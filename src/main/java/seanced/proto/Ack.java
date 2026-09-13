package seanced.proto;

import java.util.List;
import java.util.Objects;

/**
 * The reply to a {@link Ping}, echoing the sequence number it answers.
 *
 * <p>Arrival is the whole signal — there is no failure variant, because a node
 * that cannot answer simply says nothing.
 */
public record Ack(NodeId from, long seqNo, List<MembershipUpdate> gossip) implements Message {

    public Ack {
        Objects.requireNonNull(from, "from");
        gossip = List.copyOf(gossip);
    }

    public Ack(NodeId from, long seqNo) {
        this(from, seqNo, List.of());
    }
}
