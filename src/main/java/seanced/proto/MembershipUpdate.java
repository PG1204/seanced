package seanced.proto;

import java.util.Objects;

/**
 * One piece of membership news: "I believe {@code node} is in {@code state}
 * as of incarnation {@code incarnation}".
 *
 * <p>These are the units of gossip. They ride piggybacked on every protocol
 * message rather than travelling in messages of their own, which is what keeps
 * SWIM's message count linear in cluster size.
 *
 * <p>The incarnation number is what makes conflicting updates resolvable. Only
 * the node itself increments its own incarnation, and it does so to refute a
 * suspicion. A higher incarnation therefore means "fresher news, straight from
 * the source" and wins over anything a third party believes.
 */
public record MembershipUpdate(NodeId node, MemberState state, long incarnation) {

    public MembershipUpdate {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(state, "state");
    }

    public static MembershipUpdate alive(NodeId node, long incarnation) {
        return new MembershipUpdate(node, MemberState.ALIVE, incarnation);
    }

    public static MembershipUpdate suspect(NodeId node, long incarnation) {
        return new MembershipUpdate(node, MemberState.SUSPECT, incarnation);
    }

    public static MembershipUpdate dead(NodeId node, long incarnation) {
        return new MembershipUpdate(node, MemberState.DEAD, incarnation);
    }

    @Override
    public String toString() {
        return node + "=" + state + "@" + incarnation;
    }
}
