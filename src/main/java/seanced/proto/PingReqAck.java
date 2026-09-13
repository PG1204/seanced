package seanced.proto;

import java.util.List;
import java.util.Objects;

/**
 * The helper's verdict on an indirect probe.
 *
 * <p>Unlike {@link Ack}, this type needs an explicit success flag. A direct ack
 * signals liveness merely by arriving, but a helper has to be able to say "I
 * tried and got nothing" — otherwise the requester cannot tell a failed probe
 * from a helper that died mid-request.
 *
 * @param from    the helper that performed the probe
 * @param target  the node that was probed
 * @param seqNo   the requester's sequence number, echoed so it can match its pending probe
 * @param reached whether the target answered the helper
 */
public record PingReqAck(NodeId from, NodeId target, long seqNo, boolean reached,
                         List<MembershipUpdate> gossip) implements Message {

    public PingReqAck {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(target, "target");
        gossip = List.copyOf(gossip);
    }

    public PingReqAck(NodeId from, NodeId target, long seqNo, boolean reached) {
        this(from, target, seqNo, reached, List.of());
    }
}
