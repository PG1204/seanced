package seanced.proto;

/**
 * Where a node sits in the SWIM lifecycle.
 *
 * <p>A node is {@code ALIVE} until a probe fails, {@code SUSPECT} while the cluster
 * waits to see whether it refutes, and {@code DEAD} once the suspicion times out.
 * {@code DEAD} is terminal: a node that is declared dead must rejoin with a fresh
 * incarnation rather than talk its way back to alive.
 */
public enum MemberState {
    ALIVE((byte) 0),
    SUSPECT((byte) 1),
    DEAD((byte) 2);

    private final byte code;

    MemberState(byte code) {
        this.code = code;
    }

    /** Stable on-the-wire code. Explicit rather than {@code ordinal()} so reordering the enum can't break the protocol. */
    public byte code() {
        return code;
    }

    public static MemberState fromCode(byte code) {
        for (MemberState s : values()) {
            if (s.code == code) return s;
        }
        throw new IllegalArgumentException("unknown member state code: " + code);
    }
}
