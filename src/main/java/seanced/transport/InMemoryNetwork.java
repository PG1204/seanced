package seanced.transport;

import seanced.clock.Clock;
import seanced.codec.MessageCodec;
import seanced.proto.Message;
import seanced.proto.NodeId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A simulated network that several nodes share, for deterministic testing.
 *
 * <p>Delivery is scheduled on the supplied {@link Clock} rather than happening
 * inline, which models latency and — more importantly — keeps a send from
 * re-entering the sender's own handler part-way through its work. With a
 * {@code FakeClock}, an entire cluster runs reproducibly on one thread.
 *
 * <p>Every failure mode SWIM is meant to tolerate can be induced here: whole-node
 * crashes, one-way and two-way link failures, network partitions, and uniform
 * packet loss.
 *
 * <p>Messages are round-tripped through {@link MessageCodec} on the way, so the
 * codec is exercised by every protocol test rather than only by its own unit
 * tests, and no test can accidentally pass by sharing a mutable object between
 * two "remote" nodes.
 */
public final class InMemoryNetwork {

    private final Clock clock;
    private final Map<NodeId, Consumer<Message>> handlers = new HashMap<>();
    private final Set<NodeId> crashed = new HashSet<>();
    private final Set<Link> blockedLinks = new HashSet<>();

    private Set<NodeId> partitionGroup = Set.of();
    private Random random = new Random(42);
    private long latencyMillis = 1;
    private double lossRate;

    private int sent;
    private int delivered;
    private int dropped;

    public InMemoryNetwork(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Creates a transport for {@code id} attached to this network. */
    public Transport join(NodeId id) {
        Objects.requireNonNull(id, "id");
        if (handlers.containsKey(id)) {
            throw new IllegalStateException("already joined: " + id);
        }
        handlers.put(id, message -> {
        });
        return new InMemoryTransport(id);
    }

    /** Stops a node sending or receiving anything, as if its process died. */
    public void crash(NodeId id) {
        crashed.add(id);
    }

    public void recover(NodeId id) {
        crashed.remove(id);
    }

    /** Blocks traffic between two nodes in both directions, leaving the rest of the cluster reachable. */
    public void blockLink(NodeId a, NodeId b) {
        blockedLinks.add(new Link(a, b));
        blockedLinks.add(new Link(b, a));
    }

    /** Blocks traffic from {@code from} to {@code to} only, leaving the reverse path open. */
    public void blockOneWay(NodeId from, NodeId to) {
        blockedLinks.add(new Link(from, to));
    }

    public void healLinks() {
        blockedLinks.clear();
    }

    /** Splits the cluster: {@code group} can talk among itself, everyone else among themselves, not across. */
    public void partition(Set<NodeId> group) {
        this.partitionGroup = new LinkedHashSet<>(group);
    }

    public void healPartition() {
        this.partitionGroup = Set.of();
    }

    /** Probability in {@code [0, 1]} that any given message is dropped. */
    public void setLossRate(double lossRate) {
        if (lossRate < 0 || lossRate > 1) {
            throw new IllegalArgumentException("lossRate must be in [0,1]: " + lossRate);
        }
        this.lossRate = lossRate;
    }

    /** One-way delay applied to every delivery. */
    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }

    /** Seeded separately from the nodes so loss patterns stay reproducible. */
    public void setRandom(Random random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    public int sentCount() {
        return sent;
    }

    public int deliveredCount() {
        return delivered;
    }

    public int droppedCount() {
        return dropped;
    }

    private boolean canReach(NodeId from, NodeId to) {
        if (crashed.contains(from) || crashed.contains(to)) {
            return false;
        }
        if (blockedLinks.contains(new Link(from, to))) {
            return false;
        }
        if (!partitionGroup.isEmpty() && partitionGroup.contains(from) != partitionGroup.contains(to)) {
            return false;
        }
        return true;
    }

    private void dispatch(NodeId from, NodeId to, Message message) {
        sent++;
        if (!canReach(from, to) || (lossRate > 0 && random.nextDouble() < lossRate)) {
            dropped++;
            return;
        }

        Consumer<Message> handler = handlers.get(to);
        if (handler == null) {
            dropped++;
            return;
        }

        // Serialise now, deserialise on arrival: the receiver gets its own object graph,
        // exactly as it would over a real socket.
        byte[] wire = MessageCodec.encode(message);
        clock.scheduleOnce(latencyMillis, () -> {
            // Re-check on arrival, so a crash during flight still loses the message.
            if (!canReach(from, to)) {
                dropped++;
                return;
            }
            delivered++;
            handlers.get(to).accept(MessageCodec.decode(wire));
        });
    }

    private record Link(NodeId from, NodeId to) {
    }

    private final class InMemoryTransport implements Transport {

        private final NodeId local;

        private InMemoryTransport(NodeId local) {
            this.local = local;
        }

        @Override
        public void send(NodeId to, Message message) {
            dispatch(local, to, message);
        }

        @Override
        public void listen(Consumer<Message> handler) {
            handlers.put(local, Objects.requireNonNull(handler, "handler"));
        }

        @Override
        public NodeId localAddress() {
            return local;
        }

        @Override
        public void close() {
            handlers.remove(local);
        }
    }
}
