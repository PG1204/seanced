package seanced;

/**
 * Protocol tuning.
 *
 * <p>The numbers trade detection speed against false-positive rate and bandwidth.
 * The defaults follow the SWIM paper's recommendations and are reasonable for a
 * LAN; a cluster spread over a WAN wants everything loosened.
 *
 * @param protocolPeriodMillis  how often each node probes one peer. The dominant knob:
 *                              halving it doubles both detection speed and traffic.
 * @param pingTimeoutMillis     how long to wait for a direct ack before falling back to
 *                              indirect probes. Must be comfortably above round-trip time
 *                              and well below the protocol period, so the indirect probe
 *                              still has room to finish inside the same period.
 * @param indirectProbeCount    how many peers are asked to probe on our behalf (<em>k</em>).
 *                              Each one is an independent network path, so this is the main
 *                              defence against declaring a node dead over one bad link.
 * @param suspicionTimeoutMillis how long a suspected node has to refute before it is
 *                              declared dead. The single biggest false-positive control:
 *                              it must exceed the worst-case time for a refutation to
 *                              propagate, which grows with cluster size.
 * @param maxGossipPerMessage   cap on piggybacked updates, keeping datagrams under the MTU.
 * @param gossipMultiplier      scales how many times each rumour is retransmitted.
 * @param deadReapMillis        how long a dead member is remembered before being forgotten.
 *                              Must far exceed rumour lifetime, or a straggling ALIVE rumour
 *                              could resurrect a node the cluster has already buried.
 */
public record SwimConfig(
        long protocolPeriodMillis,
        long pingTimeoutMillis,
        int indirectProbeCount,
        long suspicionTimeoutMillis,
        int maxGossipPerMessage,
        int gossipMultiplier,
        long deadReapMillis) {

    public SwimConfig {
        if (protocolPeriodMillis <= 0) {
            throw new IllegalArgumentException("protocolPeriodMillis must be positive");
        }
        if (pingTimeoutMillis <= 0 || pingTimeoutMillis >= protocolPeriodMillis) {
            throw new IllegalArgumentException(
                    "pingTimeoutMillis must be in (0, protocolPeriodMillis): " + pingTimeoutMillis);
        }
        if (indirectProbeCount < 0) {
            throw new IllegalArgumentException("indirectProbeCount must not be negative");
        }
        if (suspicionTimeoutMillis <= 0) {
            throw new IllegalArgumentException("suspicionTimeoutMillis must be positive");
        }
        if (maxGossipPerMessage < 0) {
            throw new IllegalArgumentException("maxGossipPerMessage must not be negative");
        }
        if (gossipMultiplier < 1) {
            throw new IllegalArgumentException("gossipMultiplier must be at least 1");
        }
        if (deadReapMillis <= 0) {
            throw new IllegalArgumentException("deadReapMillis must be positive");
        }
    }

    /** LAN defaults: detects a crash in roughly 6 seconds. */
    public static SwimConfig defaults() {
        return new SwimConfig(1_000, 300, 3, 5_000, 6, 4, 300_000);
    }

    /**
     * Aggressive settings for tests: everything shrunk so simulated time passes quickly.
     * Reaping is left long so that tests observing dead state aren't racing the reaper;
     * the test that covers reaping shortens it explicitly.
     */
    public static SwimConfig forTesting() {
        return new SwimConfig(1_000, 200, 2, 3_000, 8, 5, 600_000);
    }

    public Builder toBuilder() {
        return new Builder()
                .protocolPeriodMillis(protocolPeriodMillis)
                .pingTimeoutMillis(pingTimeoutMillis)
                .indirectProbeCount(indirectProbeCount)
                .suspicionTimeoutMillis(suspicionTimeoutMillis)
                .maxGossipPerMessage(maxGossipPerMessage)
                .gossipMultiplier(gossipMultiplier)
                .deadReapMillis(deadReapMillis);
    }

    public static Builder builder() {
        return SwimConfig.defaults().toBuilder();
    }

    /** Mutable builder, so callers can override one field without restating the rest. */
    public static final class Builder {
        private long protocolPeriodMillis = 1_000;
        private long pingTimeoutMillis = 300;
        private int indirectProbeCount = 3;
        private long suspicionTimeoutMillis = 5_000;
        private int maxGossipPerMessage = 6;
        private int gossipMultiplier = 4;
        private long deadReapMillis = 300_000;

        public Builder protocolPeriodMillis(long value) {
            this.protocolPeriodMillis = value;
            return this;
        }

        public Builder pingTimeoutMillis(long value) {
            this.pingTimeoutMillis = value;
            return this;
        }

        public Builder indirectProbeCount(int value) {
            this.indirectProbeCount = value;
            return this;
        }

        public Builder suspicionTimeoutMillis(long value) {
            this.suspicionTimeoutMillis = value;
            return this;
        }

        public Builder maxGossipPerMessage(int value) {
            this.maxGossipPerMessage = value;
            return this;
        }

        public Builder gossipMultiplier(int value) {
            this.gossipMultiplier = value;
            return this;
        }

        public Builder deadReapMillis(long value) {
            this.deadReapMillis = value;
            return this;
        }

        public SwimConfig build() {
            return new SwimConfig(protocolPeriodMillis, pingTimeoutMillis, indirectProbeCount,
                    suspicionTimeoutMillis, maxGossipPerMessage, gossipMultiplier, deadReapMillis);
        }
    }
}
