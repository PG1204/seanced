package seanced.membership;

/**
 * Notified when the cluster's shape changes.
 *
 * <p>All methods default to no-ops so implementations can override only what they
 * care about. Callbacks run on the caller's thread inside the protocol's own
 * processing, so they must be quick and must not block — hand work off to your
 * own executor if it might take a while.
 */
public interface MembershipListener {

    /** A node was seen for the first time, or came back after being declared dead. */
    default void onJoin(Member member) {
    }

    /** A node stopped answering probes and is now on the clock to refute. */
    default void onSuspect(Member member) {
    }

    /** A node failed to refute in time and is now considered gone. */
    default void onDead(Member member) {
    }

    /** A suspected node refuted successfully and is alive again. */
    default void onRecover(Member member) {
    }
}
