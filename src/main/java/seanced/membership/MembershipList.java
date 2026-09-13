package seanced.membership;

import seanced.proto.MemberState;
import seanced.proto.MembershipUpdate;
import seanced.proto.NodeId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This node's view of the cluster, and the rules for updating it.
 *
 * <p>The interesting part is {@link #apply}. Updates arrive out of order, from
 * multiple hops, carrying conflicting claims — "B is alive", "B is suspect" —
 * and every node must resolve them the same way or the cluster never converges.
 * Incarnation numbers make that possible: only a node increments its own, and it
 * does so to refute a suspicion, so a higher incarnation is authoritative.
 *
 * <p>The precedence rules, from the SWIM paper:
 * <ul>
 *   <li><b>ALIVE(i)</b> beats ALIVE(j) and SUSPECT(j) when {@code i > j}. A node must
 *       out-run a suspicion, not merely match it.</li>
 *   <li><b>SUSPECT(i)</b> beats ALIVE(j) when {@code i >= j}, and SUSPECT(j) when
 *       {@code i > j}. Equality favours suspicion, so one failed probe is enough to
 *       start the clock without the node having to lose a race.</li>
 *   <li><b>DEAD(i)</b> beats everything. Death is terminal; a node that wants back in
 *       must restart and rejoin.</li>
 * </ul>
 *
 * <p>Not thread-safe. {@code SwimNode} serialises all access.
 */
public final class MembershipList {

    private final NodeId self;
    private final Map<NodeId, Member> members = new LinkedHashMap<>();
    private final List<MembershipListener> listeners = new CopyOnWriteArrayList<>();

    /** Shuffled round-robin order for probing, rebuilt each time it is exhausted. */
    private final List<NodeId> probeOrder = new ArrayList<>();
    private int probeIndex;

    /** Cursor for anti-entropy, so repeated samples cycle through the table rather than repeating. */
    private int gossipCursor;

    public MembershipList(NodeId self) {
        this.self = Objects.requireNonNull(self, "self");
        members.put(self, new Member(self, MemberState.ALIVE, 0));
    }

    public void addListener(MembershipListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Merges one update into the view.
     *
     * <p>Updates about this node are rejected outright — only {@code SwimNode} may
     * change the local entry, via {@link #updateSelf}, and only by refuting.
     *
     * @return {@code true} if the view changed, which means the update is news worth gossiping on
     */
    public boolean apply(MembershipUpdate update) {
        Objects.requireNonNull(update, "update");
        if (update.node().equals(self)) {
            return false;
        }

        Member current = members.get(update.node());
        if (current == null) {
            Member added = new Member(update.node(), update.state(), update.incarnation());
            members.put(update.node(), added);
            notifyOf(null, added);
            return true;
        }

        if (!overrides(update, current)) {
            return false;
        }

        Member updated = new Member(update.node(), update.state(), update.incarnation());
        members.put(update.node(), updated);
        notifyOf(current, updated);
        return true;
    }

    private static boolean overrides(MembershipUpdate update, Member current) {
        return switch (update.state()) {
            // A higher incarnation beats anything, death included. Death is still
            // effectively terminal, because only the node itself raises its own
            // incarnation and a genuinely dead node cannot — but a live node wrongly
            // buried during a partition can rescue itself once it hears about it.
            // Making death strictly final instead would let one side of a healed
            // partition permanently poison the other, since DEAD outranks everything
            // and would spread back as soon as the link recovered.
            case ALIVE -> update.incarnation() > current.incarnation();
            case SUSPECT -> switch (current.state()) {
                case ALIVE -> update.incarnation() >= current.incarnation();
                case SUSPECT -> update.incarnation() > current.incarnation();
                case DEAD -> false;
            };
            // Equality favours death, so a failed probe needs no incarnation bump to
            // take effect — but a stale claim about an older incarnation is ignored.
            case DEAD -> current.state() != MemberState.DEAD
                    && update.incarnation() >= current.incarnation();
        };
    }

    private void notifyOf(Member before, Member after) {
        if (listeners.isEmpty()) {
            return;
        }
        for (MembershipListener listener : listeners) {
            try {
                if (before == null || (before.isDead() && after.isAlive())) {
                    listener.onJoin(after);
                } else if (after.state() == MemberState.SUSPECT) {
                    listener.onSuspect(after);
                } else if (after.isDead()) {
                    listener.onDead(after);
                } else if (before.state() == MemberState.SUSPECT && after.isAlive()) {
                    listener.onRecover(after);
                }
            } catch (RuntimeException e) {
                System.getLogger(MembershipList.class.getName())
                        .log(System.Logger.Level.WARNING, "membership listener threw", e);
            }
        }
    }

    /** Records this node's own refutation at a new, higher incarnation. */
    public void updateSelf(long incarnation) {
        members.put(self, new Member(self, MemberState.ALIVE, incarnation));
    }

    public Optional<Member> get(NodeId id) {
        return Optional.ofNullable(members.get(id));
    }

    public long incarnationOf(NodeId id) {
        Member member = members.get(id);
        return member == null ? 0 : member.incarnation();
    }

    public NodeId self() {
        return self;
    }

    /** Every member including this node and any known-dead ones. */
    public List<Member> all() {
        return List.copyOf(members.values());
    }

    /** Members currently believed alive, including this node. */
    public List<Member> alive() {
        return members.values().stream().filter(Member::isAlive).toList();
    }

    /** Count of members believed alive, including this node. */
    public int aliveCount() {
        return (int) members.values().stream().filter(Member::isAlive).count();
    }

    public int size() {
        return members.size();
    }

    /**
     * Picks the next peer to probe, cycling through a shuffled order.
     *
     * <p>Round-robin over a shuffle rather than an independent random pick each
     * period: it gives every member a bounded worst-case time to be probed, which
     * bounds detection time. Independent picks would let an unlucky node go
     * unprobed for arbitrarily long.
     */
    public Optional<NodeId> nextProbeTarget(Random random) {
        for (int attempt = 0; attempt < 2; attempt++) {
            while (probeIndex < probeOrder.size()) {
                NodeId candidate = probeOrder.get(probeIndex++);
                Member member = members.get(candidate);
                // The order is a snapshot; skip anyone who has died since it was built.
                if (member != null && !member.isDead()) {
                    return Optional.of(candidate);
                }
            }
            rebuildProbeOrder(random);
            if (probeOrder.isEmpty()) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private void rebuildProbeOrder(Random random) {
        probeOrder.clear();
        for (Member member : members.values()) {
            if (!member.id().equals(self) && !member.isDead()) {
                probeOrder.add(member.id());
            }
        }
        Collections.shuffle(probeOrder, random);
        probeIndex = 0;
    }

    /** Up to {@code count} random live peers, excluding this node and anything in {@code exclude}. */
    public List<NodeId> randomAlivePeers(int count, Random random, Collection<NodeId> exclude) {
        List<NodeId> candidates = new ArrayList<>();
        for (Member member : members.values()) {
            if (member.isAlive() && !member.id().equals(self) && !exclude.contains(member.id())) {
                candidates.add(member.id());
            }
        }
        Collections.shuffle(candidates, random);
        return List.copyOf(candidates.subList(0, Math.min(count, candidates.size())));
    }

    /**
     * A rolling sample of current state, used to top up gossip that has spare room.
     *
     * <p>This is lightweight anti-entropy. Rumours expire after a bounded number of
     * retransmissions, so on an unlucky day a node can miss one permanently; folding
     * current state into otherwise-empty gossip slots repairs that without any extra
     * messages. Real SWIM implementations do the same thing more thoroughly with a
     * periodic full state sync over TCP.
     */
    public List<MembershipUpdate> antiEntropySample(int count, Set<NodeId> exclude) {
        if (count <= 0 || members.isEmpty()) {
            return List.of();
        }
        List<Member> snapshot = new ArrayList<>(members.values());
        List<MembershipUpdate> sample = new ArrayList<>(Math.min(count, snapshot.size()));

        for (int i = 0; i < snapshot.size() && sample.size() < count; i++) {
            Member member = snapshot.get(Math.floorMod(gossipCursor + i, snapshot.size()));
            // Dead members are deliberately left out. Death travels as a rumour, which
            // expires; re-asserting it forever would mean a node that had already reaped
            // an entry immediately re-learned it from a peer, and nothing would ever be
            // forgotten. Excluding it here lets reaping actually stick.
            if (!member.isDead() && !exclude.contains(member.id())) {
                sample.add(member.asUpdate());
            }
        }
        gossipCursor = Math.floorMod(gossipCursor + sample.size() + 1, Math.max(1, snapshot.size()));
        return sample;
    }

    /** Forgets a dead member entirely, so long-running clusters with churn don't grow without bound. */
    public void remove(NodeId id) {
        if (!id.equals(self)) {
            members.remove(id);
        }
    }

    @Override
    public String toString() {
        return "MembershipList(self=" + self + ", members=" + members.values() + ")";
    }
}
