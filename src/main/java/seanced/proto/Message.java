package seanced.proto;

import java.util.List;

/**
 * A SWIM protocol message.
 *
 * <p>Sealed to exactly the four message types the protocol defines, which lets
 * the dispatch in {@code SwimNode} and {@code MessageCodec} switch exhaustively
 * without a default branch — add a fifth type and the compiler points at every
 * place that must handle it.
 *
 * <p>Every message carries gossip. That is the central trick of SWIM: membership
 * updates never get their own broadcast, they hitch a ride on the failure
 * detector's traffic.
 */
public sealed interface Message
        permits Ping, Ack, PingReq, PingReqAck {

    /** The node that sent this particular message. Not necessarily the node that started the exchange. */
    NodeId from();

    /**
     * Correlates a request with its reply.
     *
     * <p>For {@link Ping}/{@link Ack} it is the prober's own counter. For
     * {@link PingReq}/{@link PingReqAck} it is the <em>requester's</em> counter,
     * echoed back by the helper — the helper uses a separate sequence number for
     * the ping it sends on the requester's behalf.
     */
    long seqNo();

    /** Membership updates piggybacked on this message. Always non-null, possibly empty, always immutable. */
    List<MembershipUpdate> gossip();
}
