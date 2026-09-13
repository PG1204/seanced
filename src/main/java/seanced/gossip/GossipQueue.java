package seanced.gossip;

import seanced.proto.MembershipUpdate;
import seanced.proto.NodeId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Holds membership news waiting to be spread, and decides what goes in each message.
 *
 * <p>Two rules do the work:
 *
 * <p><b>One rumour per node.</b> Newer news about a node replaces older news about
 * the same node, and resets its transmission count. Without this the queue would
 * fill with a node's entire history instead of its current state.
 *
 * <p><b>Bounded retransmission.</b> Each rumour is sent a limited number of times,
 * then dropped. The limit scales with {@code log(n)} because that is how many rounds
 * an epidemic needs to reach an entire population — enough for high-probability
 * coverage, while keeping the queue from growing without bound. Least-transmitted
 * rumours go out first, so fresh news is not starved by older news still in flight.
 */
public final class GossipQueue {

    private final Map<NodeId, Entry> rumors = new LinkedHashMap<>();
    private final int multiplier;

    /**
     * @param multiplier scales the retransmission limit. Higher means more redundancy
     *                   and faster convergence at the cost of larger messages; the SWIM
     *                   paper's implementation uses a small constant, typically 3-4.
     */
    public GossipQueue(int multiplier) {
        if (multiplier < 1) {
            throw new IllegalArgumentException("multiplier must be at least 1: " + multiplier);
        }
        this.multiplier = multiplier;
    }

    /** Queues news, replacing anything currently held about the same node. */
    public void add(MembershipUpdate update) {
        Objects.requireNonNull(update, "update");
        rumors.put(update.node(), new Entry(update));
    }

    /**
     * Takes up to {@code max} rumours to piggyback on an outgoing message, counting
     * the transmission and retiring anything that has been sent enough times.
     *
     * @param clusterSize current member count, which sets the retransmission limit
     */
    public List<MembershipUpdate> drain(int max, int clusterSize) {
        if (max <= 0 || rumors.isEmpty()) {
            return List.of();
        }

        int limit = retransmitLimit(clusterSize);
        List<Entry> candidates = new ArrayList<>(rumors.values());
        // Least-transmitted first, so new rumours overtake ones already well spread.
        candidates.sort(Comparator.comparingInt(entry -> entry.transmissions));

        List<MembershipUpdate> batch = new ArrayList<>(Math.min(max, candidates.size()));
        for (Entry entry : candidates) {
            if (batch.size() >= max) {
                break;
            }
            batch.add(entry.update);
            entry.transmissions++;
            if (entry.transmissions >= limit) {
                rumors.remove(entry.update.node());
            }
        }
        return List.copyOf(batch);
    }

    /**
     * How many times one rumour is retransmitted: {@code ceil(multiplier * log10(n + 1))}.
     *
     * <p>Logarithmic because gossip spreads exponentially — each round roughly doubles
     * the number of nodes that know, so {@code log(n)} rounds suffice for the whole
     * cluster. A linear limit would waste bandwidth on large clusters and a constant
     * one would fail to converge on them.
     */
    public int retransmitLimit(int clusterSize) {
        return Math.max(1, (int) Math.ceil(multiplier * Math.log10(Math.max(1, clusterSize) + 1.0)));
    }

    public int size() {
        return rumors.size();
    }

    public boolean isEmpty() {
        return rumors.isEmpty();
    }

    public void clear() {
        rumors.clear();
    }

    private static final class Entry {
        private final MembershipUpdate update;
        private int transmissions;

        private Entry(MembershipUpdate update) {
            this.update = update;
        }
    }
}
