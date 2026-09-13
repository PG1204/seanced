package seanced.proto;

import java.util.List;
import java.util.Objects;

/**
 * An indirect probe request: "I can't reach {@code target} — can you?"
 *
 * <p>Sent to <em>k</em> random peers after a direct {@link Ping} times out. This
 * is what keeps SWIM from raising a false alarm every time one link drops a
 * packet: the target is only suspected if several independent paths all fail.
 *
 * @param from   the requester, who could not reach the target and wants help
 * @param target the node under suspicion, which the recipient should ping
 * @param seqNo  the <em>requester's</em> sequence number, echoed back in the {@link PingReqAck}
 */
public record PingReq(NodeId from, NodeId target, long seqNo, List<MembershipUpdate> gossip) implements Message {

    public PingReq {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(target, "target");
        gossip = List.copyOf(gossip);
    }

    public PingReq(NodeId from, NodeId target, long seqNo) {
        this(from, target, seqNo, List.of());
    }
}
