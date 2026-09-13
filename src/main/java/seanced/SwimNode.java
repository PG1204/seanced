package seanced;

import seanced.clock.Clock;
import seanced.gossip.GossipQueue;
import seanced.membership.Member;
import seanced.membership.MembershipList;
import seanced.membership.MembershipListener;
import seanced.proto.Ack;
import seanced.proto.MemberState;
import seanced.proto.MembershipUpdate;
import seanced.proto.Message;
import seanced.proto.NodeId;
import seanced.proto.Ping;
import seanced.proto.PingReq;
import seanced.proto.PingReqAck;
import seanced.transport.Transport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * One cluster member: runs the failure detector, spreads gossip, and maintains
 * this node's view of who is alive.
 *
 * <p>Each protocol period it picks one peer and probes it:
 *
 * <ol>
 *   <li>Send a {@link Ping}, wait {@code pingTimeoutMillis} for an {@link Ack}.</li>
 *   <li>On timeout, ask <em>k</em> peers to probe the target for us with {@link PingReq}.
 *       Their independent network paths are what distinguish "this link is bad" from
 *       "that node is gone".</li>
 *   <li>If nothing answers by the end of the period, mark the target {@code SUSPECT}
 *       and gossip it.</li>
 *   <li>If the target does not refute within {@code suspicionTimeoutMillis}, declare
 *       it {@code DEAD}.</li>
 * </ol>
 *
 * <p>Every message sent carries gossip, so membership news spreads on the back of
 * failure-detector traffic rather than in broadcasts of its own. That is what keeps
 * SWIM's per-node message load constant as the cluster grows.
 *
 * <p>All entry points are synchronised. With a {@code FakeClock} everything already
 * runs on one thread, but in production the clock's scheduler thread and the
 * transport's receiver thread both call in, and the state here is plain mutable
 * collections.
 */
public final class SwimNode implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(SwimNode.class.getName());

    private final NodeId self;
    private final SwimConfig config;
    private final Clock clock;
    private final Transport transport;
    private final Random random;
    private final MembershipList members;
    private final GossipQueue gossip;

    /** Probes awaiting a reply, keyed by the sequence number we sent. */
    private final Map<Long, Probe> pendingProbes = new HashMap<>();

    private long incarnation;
    private long nextSeqNo;
    private boolean running;

    public SwimNode(NodeId self, SwimConfig config, Clock clock, Transport transport) {
        this(self, config, clock, transport, new Random());
    }

    /** @param random seed it for reproducible tests; the protocol's peer selection is random by design */
    public SwimNode(NodeId self, SwimConfig config, Clock clock, Transport transport, Random random) {
        this.self = Objects.requireNonNull(self, "self");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.random = Objects.requireNonNull(random, "random");
        this.members = new MembershipList(self);
        this.gossip = new GossipQueue(config.gossipMultiplier());
    }

    public NodeId id() {
        return self;
    }

    public MembershipList membership() {
        return members;
    }

    public void addListener(MembershipListener listener) {
        members.addListener(listener);
    }

    /** Starts receiving messages and running protocol periods. */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        transport.listen(this::onMessage);
        scheduleNextPeriod();
    }

    /**
     * Learns about seed nodes so the first probe has somewhere to go.
     *
     * <p>Nothing else is needed to join: once this node probes a seed, the seed learns
     * of it from the message's sender field, gossips it onward, and the rest of the
     * cluster finds out within a few periods.
     */
    public synchronized void join(NodeId... seeds) {
        for (NodeId seed : seeds) {
            if (!seed.equals(self)) {
                learn(seed);
            }
        }
    }

    /**
     * Announces departure before shutting down.
     *
     * <p>A node that just exits is detected as failed, which costs the cluster a
     * suspicion timeout of uncertainty. Declaring ourselves dead on the way out lets
     * peers remove us immediately. The incarnation is bumped so the announcement
     * outranks any ALIVE rumour about us still circulating.
     */
    public synchronized void leave() {
        if (!running) {
            return;
        }
        incarnation++;
        MembershipUpdate farewell = MembershipUpdate.dead(self, incarnation);

        // Told to everyone we know, not to a gossip-sized sample. This happens once, at
        // shutdown, and the whole point is for peers to hear it before they would have
        // detected the failure anyway — relying on gossip to carry it would give up most
        // of that head start.
        for (NodeId peer : members.randomAlivePeers(Integer.MAX_VALUE, random, Set.of())) {
            transport.send(peer, new Ping(self, nextSeqNo++, List.of(farewell)));
        }
        running = false;
    }

    public synchronized void stop() {
        running = false;
    }

    @Override
    public synchronized void close() {
        running = false;
        transport.close();
    }

    // ---------------------------------------------------------------- protocol period

    private void scheduleNextPeriod() {
        clock.scheduleOnce(config.protocolPeriodMillis(), this::runProtocolPeriod);
    }

    private synchronized void runProtocolPeriod() {
        if (!running) {
            return;
        }
        try {
            members.nextProbeTarget(random).ifPresent(this::probe);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "protocol period failed on " + self, e);
        } finally {
            // Rescheduled even on failure: one bad period must not stop the detector forever.
            scheduleNextPeriod();
        }
    }

    private void probe(NodeId target) {
        long seqNo = nextSeqNo++;
        pendingProbes.put(seqNo, Probe.direct(target));
        send(target, gossipList -> new Ping(self, seqNo, gossipList));
        clock.scheduleOnce(config.pingTimeoutMillis(), () -> onDirectProbeTimeout(seqNo));
    }

    private synchronized void onDirectProbeTimeout(long seqNo) {
        Probe probe = pendingProbes.get(seqNo);
        if (probe == null || probe.completed) {
            return;
        }

        // No direct answer. Ask k peers to try the same target over their own paths.
        List<NodeId> helpers = members.randomAlivePeers(
                config.indirectProbeCount(), random, List.of(probe.target));
        for (NodeId helper : helpers) {
            send(helper, gossipList -> new PingReq(self, probe.target, seqNo, gossipList));
        }

        long remaining = config.protocolPeriodMillis() - config.pingTimeoutMillis();
        clock.scheduleOnce(remaining, () -> onIndirectProbeTimeout(seqNo));
    }

    private synchronized void onIndirectProbeTimeout(long seqNo) {
        Probe probe = pendingProbes.remove(seqNo);
        if (probe == null || probe.completed) {
            return;
        }
        // Neither we nor any helper could reach the target: suspect it.
        suspect(probe.target);
    }

    private void suspect(NodeId target) {
        Optional<Member> existing = members.get(target);
        if (existing.isEmpty() || existing.get().state() != MemberState.ALIVE) {
            return;
        }

        long suspectedIncarnation = existing.get().incarnation();
        MembershipUpdate update = MembershipUpdate.suspect(target, suspectedIncarnation);
        if (members.apply(update)) {
            gossip.add(update);
            scheduleDeathConfirmation(target, suspectedIncarnation);
        }
    }

    /**
     * Promotes a suspicion to death once the refutation window closes.
     *
     * <p>The incarnation is captured when suspicion starts and rechecked when the timer
     * fires. If the node refuted in the meantime it will have a higher incarnation, and
     * this becomes a no-op — which is how a live node that was merely unreachable for a
     * moment escapes being declared dead.
     */
    private void scheduleDeathConfirmation(NodeId target, long suspectedIncarnation) {
        clock.scheduleOnce(config.suspicionTimeoutMillis(), () -> confirmDeath(target, suspectedIncarnation));
    }

    private synchronized void confirmDeath(NodeId target, long suspectedIncarnation) {
        members.get(target).ifPresent(member -> {
            if (member.state() != MemberState.SUSPECT || member.incarnation() != suspectedIncarnation) {
                return;
            }
            MembershipUpdate dead = MembershipUpdate.dead(target, suspectedIncarnation);
            if (members.apply(dead)) {
                gossip.add(dead);
                clock.scheduleOnce(config.deadReapMillis(), () -> reap(target, suspectedIncarnation));
            }
        });
    }

    private synchronized void reap(NodeId target, long deadIncarnation) {
        members.get(target).ifPresent(member -> {
            if (member.isDead() && member.incarnation() == deadIncarnation) {
                members.remove(target);
            }
        });
    }

    // ---------------------------------------------------------------- inbound

    private synchronized void onMessage(Message message) {
        if (!running) {
            return;
        }
        try {
            learn(message.from());
            applyGossip(message.gossip());

            switch (message) {
                case Ping ping -> send(ping.from(), gossipList -> new Ack(self, ping.seqNo(), gossipList));
                case Ack ack -> completeProbe(ack.seqNo());
                case PingReq pingReq -> relayProbe(pingReq);
                case PingReqAck pingReqAck -> {
                    if (pingReqAck.reached()) {
                        completeProbe(pingReqAck.seqNo());
                    }
                    // A failed indirect probe is not acted on directly. The requester lets the
                    // period expire and suspects then, so one helper's bad luck is not decisive.
                }
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "failed handling " + message + " on " + self, e);
        }
    }

    /**
     * Performs a probe on another node's behalf.
     *
     * <p>The helper uses its own sequence number for the ping it sends, and remembers
     * the requester's so it can be echoed back. Two independent counters: conflating
     * them would let two requesters' probes collide.
     */
    private void relayProbe(PingReq request) {
        long relaySeqNo = nextSeqNo++;
        pendingProbes.put(relaySeqNo, Probe.relayed(request.target(), request.from(), request.seqNo()));
        send(request.target(), gossipList -> new Ping(self, relaySeqNo, gossipList));

        clock.scheduleOnce(config.pingTimeoutMillis(), () -> onRelayTimeout(relaySeqNo));
    }

    private synchronized void onRelayTimeout(long relaySeqNo) {
        Probe probe = pendingProbes.remove(relaySeqNo);
        if (probe == null || probe.completed) {
            return;
        }
        // Report the failure explicitly; silence would be indistinguishable from us dying.
        send(probe.relayTo, gossipList ->
                new PingReqAck(self, probe.target, probe.relaySeqNo, false, gossipList));
    }

    private void completeProbe(long seqNo) {
        Probe probe = pendingProbes.remove(seqNo);
        if (probe == null || probe.completed) {
            return;
        }
        probe.completed = true;

        if (probe.relayTo != null) {
            send(probe.relayTo, gossipList ->
                    new PingReqAck(self, probe.target, probe.relaySeqNo, true, gossipList));
        }
        // A successful direct probe needs no membership change: the target is already
        // alive in our view, and if it is suspected only the node itself may refute.
    }

    private void applyGossip(List<MembershipUpdate> updates) {
        for (MembershipUpdate update : updates) {
            if (update.node().equals(self)) {
                refute(update);
                continue;
            }
            if (members.apply(update)) {
                // News to us, so it is news to our peers: pass it on.
                gossip.add(update);
                if (update.state() == MemberState.SUSPECT) {
                    scheduleDeathConfirmation(update.node(), update.incarnation());
                } else if (update.state() == MemberState.DEAD) {
                    clock.scheduleOnce(config.deadReapMillis(),
                            () -> reap(update.node(), update.incarnation()));
                }
            }
        }
    }

    /**
     * Answers a claim about this node.
     *
     * <p>A node is the only authority on its own liveness. Hearing itself called
     * suspect or dead, it raises its incarnation past whatever the rumour carried and
     * gossips ALIVE at the new number — which outranks the rumour everywhere it has
     * spread. This is the mechanism that makes a transient network problem recoverable
     * rather than fatal.
     */
    private void refute(MembershipUpdate claim) {
        if (claim.state() == MemberState.ALIVE) {
            return;
        }
        if (claim.incarnation() >= incarnation) {
            incarnation = claim.incarnation() + 1;
        }
        members.updateSelf(incarnation);
        gossip.add(MembershipUpdate.alive(self, incarnation));
        LOG.log(System.Logger.Level.DEBUG, "{0} refuting {1} at incarnation {2}",
                self, claim.state(), incarnation);
    }

    /** Records a node we have just heard from, in case gossip has not reached us yet. */
    private void learn(NodeId peer) {
        if (peer.equals(self) || members.get(peer).isPresent()) {
            return;
        }
        MembershipUpdate update = MembershipUpdate.alive(peer, 0);
        if (members.apply(update)) {
            gossip.add(update);
        }
    }

    // ---------------------------------------------------------------- outbound

    /**
     * Builds and sends a message, attaching gossip.
     *
     * <p>The message is constructed by callback because its gossip must be drained at
     * send time — draining increments transmission counts, so it has to happen once per
     * message actually sent.
     */
    private void send(NodeId to, java.util.function.Function<List<MembershipUpdate>, Message> factory) {
        transport.send(to, factory.apply(outgoingGossip()));
    }

    /**
     * Selects what rides along on the next message: pending rumours first, then current
     * membership state to fill any spare capacity.
     *
     * <p>The top-up is cheap anti-entropy. Rumours retire after a bounded number of
     * transmissions, so a node that was unlucky during those rounds would otherwise stay
     * wrong indefinitely. Folding in current state costs nothing — the slots would have
     * gone out empty — and guarantees the cluster converges.
     */
    private List<MembershipUpdate> outgoingGossip() {
        int capacity = config.maxGossipPerMessage();
        if (capacity == 0) {
            return List.of();
        }

        List<MembershipUpdate> outgoing = new ArrayList<>(gossip.drain(capacity, members.size()));

        int room = capacity - outgoing.size();
        if (room > 0) {
            Set<NodeId> alreadyIncluded = new HashSet<>();
            for (MembershipUpdate update : outgoing) {
                alreadyIncluded.add(update.node());
            }
            outgoing.addAll(members.antiEntropySample(room, alreadyIncluded));
        }
        return List.copyOf(outgoing);
    }

    /** Current incarnation, raised each time this node refutes a suspicion about itself. */
    public synchronized long incarnation() {
        return incarnation;
    }

    @Override
    public String toString() {
        return "SwimNode(" + self + ")";
    }

    /**
     * An outstanding probe.
     *
     * <p>{@code relayTo} distinguishes the two kinds: null for a probe of our own,
     * non-null when we are probing for someone else and owe them a {@link PingReqAck}.
     */
    private static final class Probe {
        private final NodeId target;
        private final NodeId relayTo;
        private final long relaySeqNo;
        private boolean completed;

        private Probe(NodeId target, NodeId relayTo, long relaySeqNo) {
            this.target = target;
            this.relayTo = relayTo;
            this.relaySeqNo = relaySeqNo;
        }

        static Probe direct(NodeId target) {
            return new Probe(target, null, 0);
        }

        static Probe relayed(NodeId target, NodeId relayTo, long relaySeqNo) {
            return new Probe(target, relayTo, relaySeqNo);
        }
    }
}
