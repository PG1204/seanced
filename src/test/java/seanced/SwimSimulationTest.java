package seanced;

import org.junit.jupiter.api.Test;
import seanced.clock.FakeClock;
import seanced.membership.Member;
import seanced.proto.MemberState;
import seanced.proto.NodeId;
import seanced.transport.InMemoryNetwork;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Randomized simulation testing: run the protocol through thousands of fault schedules
 * nobody thought to write by hand, and assert the invariants that must hold regardless.
 *
 * <p>Each run builds a cluster of random size, subjects it to a random sequence of
 * crashes, restarts, partitions, link failures and packet loss, then removes every fault
 * and lets the cluster settle. The property under test is <em>eventual convergence</em>:
 * once the network is healthy again, every live node must agree that every live node is
 * alive. A cluster that cannot recover from a fault it survived is as broken as one that
 * crashes.
 *
 * <p>This works because every source of nondeterminism is injected. A seed fixes the
 * cluster size, the fault schedule, the loss pattern and each node's peer selection, so a
 * failure reported here reproduces exactly — {@link #reproduceSingleSeed()} replays one.
 * That is the difference between a fuzzer and a flaky test.
 */
class SwimSimulationTest {

    /**
     * Kept modest so the suite stays fast — this runs in a few seconds. Raise it locally
     * when hunting for rare bugs; 5000 seeds takes about 25 seconds and is a reasonable
     * pre-release sweep.
     */
    private static final int RUNS = 1000;

    @Test
    void convergesFromEveryRandomizedFaultSchedule() {
        List<String> failures = new ArrayList<>();

        for (int seed = 0; seed < RUNS; seed++) {
            try {
                new Simulation(seed).run();
            } catch (AssertionError | RuntimeException e) {
                failures.add("seed " + seed + ": " + e.getMessage());
            }
        }

        if (!failures.isEmpty()) {
            // Report every failing seed, not just the first: the shape of the set says
            // more about the bug than any one instance does.
            fail(failures.size() + " of " + RUNS + " simulations failed:\n  "
                    + String.join("\n  ", failures.subList(0, Math.min(10, failures.size()))));
        }
    }

    /**
     * Replays one seed on its own. When the sweep above reports a failure, change this
     * seed to that one and debug a single deterministic run.
     */
    @Test
    void reproduceSingleSeed() {
        new Simulation(0).run();
    }

    /** One deterministic cluster lifetime, driven entirely by its seed. */
    private static final class Simulation {

        private static final long SETTLE_MILLIS = 30_000;
        private static final long FAULT_WINDOW_MILLIS = 20_000;
        private static final long RECOVERY_MILLIS = 400_000;

        private final Random random;
        private final FakeClock clock = new FakeClock();
        private final InMemoryNetwork network;
        private final List<SwimNode> nodes = new ArrayList<>();
        private final SwimConfig config;
        private final List<String> history = new ArrayList<>();

        Simulation(int seed) {
            this.random = new Random(seed);
            this.network = new InMemoryNetwork(clock);

            int size = 3 + random.nextInt(6);
            // Suspicion timeout scales with the fault window so a node always has room to
            // refute once the network recovers. Too short and the test would be asserting
            // that a misconfigured cluster converges, which is not a property SWIM has.
            this.config = SwimConfig.forTesting().toBuilder()
                    .suspicionTimeoutMillis(6_000)
                    .build();

            for (int i = 0; i < size; i++) {
                NodeId id = new NodeId("10.0.0." + (i + 1), 7946);
                nodes.add(new SwimNode(id, config, clock, network.join(id), new Random(seed * 31L + i)));
            }
            network.setRandom(new Random(seed * 17L + 1));
            record("cluster of " + size);
        }

        void run() {
            try {
                NodeId seedNode = nodes.get(0).id();
                for (SwimNode node : nodes) {
                    node.join(seedNode);
                    node.start();
                }
                clock.advanceBy(SETTLE_MILLIS);

                int faults = 1 + random.nextInt(4);
                for (int i = 0; i < faults; i++) {
                    injectFault();
                    clock.advanceBy(1_000 + random.nextInt((int) FAULT_WINDOW_MILLIS));
                }

                healEverything();
                clock.advanceBy(RECOVERY_MILLIS);

                assertConverged();
            } finally {
                nodes.forEach(SwimNode::close);
            }
        }

        private void injectFault() {
            switch (random.nextInt(5)) {
                case 0 -> {
                    NodeId victim = randomNode();
                    network.crash(victim);
                    record("crash " + victim);
                }
                case 1 -> {
                    // A crash the cluster may or may not have noticed yet, then recovery.
                    NodeId victim = randomNode();
                    network.crash(victim);
                    clock.advanceBy(1_000 + random.nextInt(10_000));
                    network.recover(victim);
                    record("flap " + victim);
                }
                case 2 -> {
                    Set<NodeId> group = randomGroup();
                    network.partition(group);
                    record("partition " + group);
                }
                case 3 -> {
                    NodeId a = randomNode();
                    NodeId b = randomNode();
                    if (!a.equals(b)) {
                        network.blockLink(a, b);
                        record("block " + a + " <-> " + b);
                    }
                }
                case 4 -> {
                    double loss = 0.05 + random.nextDouble() * 0.25;
                    network.setLossRate(loss);
                    record(String.format("loss %.2f", loss));
                }
                default -> throw new IllegalStateException();
            }
        }

        /**
         * Restores a perfect network. Any node crashed and never recovered is brought
         * back too, so the end state is one every node should agree on.
         */
        private void healEverything() {
            network.healPartition();
            network.healLinks();
            network.setLossRate(0);
            for (SwimNode node : nodes) {
                network.recover(node.id());
            }
            record("healed");
        }

        private void assertConverged() {
            for (SwimNode observer : nodes) {
                for (SwimNode subject : nodes) {
                    MemberState state = observer.membership().get(subject.id())
                            .map(Member::state)
                            .orElse(null);
                    if (state != MemberState.ALIVE) {
                        throw new AssertionError(describe(observer, subject, state));
                    }
                }
            }
        }

        private String describe(SwimNode observer, SwimNode subject, MemberState state) {
            return observer.id() + " sees " + subject.id() + " as " + state
                    + " after healing (expected ALIVE)"
                    + "\n      history: " + String.join(" | ", history)
                    + "\n      view:    " + observer.membership().all();
        }

        private NodeId randomNode() {
            return nodes.get(random.nextInt(nodes.size())).id();
        }

        private Set<NodeId> randomGroup() {
            Set<NodeId> group = new LinkedHashSet<>();
            int size = 1 + random.nextInt(Math.max(1, nodes.size() - 1));
            for (int i = 0; i < size; i++) {
                group.add(nodes.get(i).id());
            }
            return group;
        }

        private void record(String event) {
            history.add("t=" + clock.nowMillis() + " " + event);
        }
    }

    /**
     * A guard on the simulation itself: a run with no faults injected must converge, or
     * the harness is broken rather than the protocol.
     */
    @Test
    void aHealthyClusterConvergesUnderTheHarness() {
        FakeClock clock = new FakeClock();
        InMemoryNetwork network = new InMemoryNetwork(clock);
        List<SwimNode> nodes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            NodeId id = new NodeId("10.0.0." + (i + 1), 7946);
            nodes.add(new SwimNode(id, SwimConfig.forTesting(), clock, network.join(id), new Random(i)));
        }
        nodes.forEach(node -> {
            node.join(nodes.get(0).id());
            node.start();
        });

        clock.advanceBy(60_000);

        for (SwimNode observer : nodes) {
            assertEquals(5, observer.membership().aliveCount(),
                    observer.id() + " should see the whole cluster alive");
        }
        nodes.forEach(SwimNode::close);
    }
}
