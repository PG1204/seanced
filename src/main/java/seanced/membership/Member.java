package seanced.membership;

import seanced.proto.MemberState;
import seanced.proto.MembershipUpdate;
import seanced.proto.NodeId;

import java.util.Objects;

/** One peer as this node currently understands it. */
public record Member(NodeId id, MemberState state, long incarnation) {

    public Member {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(state, "state");
    }

    public boolean isAlive() {
        return state == MemberState.ALIVE;
    }

    public boolean isDead() {
        return state == MemberState.DEAD;
    }

    /** This member's current belief, in the form it travels as gossip. */
    public MembershipUpdate asUpdate() {
        return new MembershipUpdate(id, state, incarnation);
    }

    @Override
    public String toString() {
        return id + "=" + state + "@" + incarnation;
    }
}
