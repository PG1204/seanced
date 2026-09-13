package seanced;

import org.junit.jupiter.api.Test;
import seanced.clock.FakeClock;
import seanced.membership.Member;
import seanced.proto.MemberState;
import seanced.proto.MembershipUpdate;
import seanced.proto.NodeId;
import seanced.proto.Ping;
import seanced.transport.InMemoryNetwork;
import seanced.transport.Transport;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end protocol tests: whole clusters running against a simulated clock and
 * network.
 *
 * <p>Every run is deterministic. Time only moves when the test advances it, message
 * delivery is scheduled on that same clock, and both the nodes and the network draw
 * from seeded {@code Random}s — so a failure here reproduces exactly rather than
 * appearing once in a hundred CI runs.
 */
class SwimClusterTest {

    private static final SwimConfig CONFIG = SwimConfig.forTesting();

    /** A cluster wired to one fake clock and one simulated network. */
    private static final class Cluster implements AutoCloseable {
        final FakeClock clock = new FakeClock();
        final InMemoryNetwork network = new InMemoryNetwork(clock);
        final List<SwimNode> nodes = new ArrayList<>();

        Cluster(int size) {
            this(size, CONFIG);
        }

        Cluster(int size, SwimConfig config) {
            for (int i = 0; i < size; i++) {
                NodeId id = new NodeId("10.0.0." + (i + 1), 7946);
                Transport transport = network.join(id);
                // Distinct seeds so nodes make independent choices, fixed so runs repeat.
                nodes.add(new SwimNode(id, config, clock, transport, new Random(1000 + i)));
            }
        }

        /** Everyone joins through node 0, then all start. Gossip spreads the rest. */
        void startAll() {
            NodeId seed = nodes.get(0).id();
            for (SwimNode node : nodes) {
                node.join(seed);
                node.start();
            }
        }

        SwimNode node(int index) {
            return nodes.get(index);
        }

        NodeId id(int index) {
            return nodes.get(index).id();
        }

        Optional<MemberState> stateOf(int observer, int subject) {
            return nodes.get(observer).membership().get(id(subject)).map(Member::state);
        }

        void run(long millis) {
            clock.advanceBy(millis);
        }

        @Override
        public void close() {
            nodes.forEach(SwimNode::close);
        }
    }

    // ------------------------------------------------------------ convergence

    @Test
    void twoNodesDiscoverEachOther() {
        try (Cluster cluster = new Cluster(2)) {
            cluster.startAll();
            cluster.run(10_000);

            assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(0, 1));
            assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(1, 0));
        }
    }

    @Test
    void allNodesConvergeOnTheFullMembership() {
        try (Cluster cluster = new Cluster(6)) {
            cluster.startAll();
            cluster.run(60_000);

            for (int observer = 0; observer < 6; observer++) {
                for (int subject = 0; subject < 6; subject++) {
                    assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(observer, subject),
                            "node " + observer + " should see node " + subject + " alive");
                }
            }
        }
    }

    @Test
    void aLateJoinerIsDiscoveredByTheWholeCluster() {
        try (Cluster cluster = new Cluster(5)) {
            // Start four nodes and let them settle, then bring the fifth in.
            for (int i = 0; i < 4; i++) {
                cluster.node(i).join(cluster.id(0));
                cluster.node(i).start();
            }
            cluster.run(20_000);

            cluster.node(4).join(cluster.id(0));
            cluster.node(4).start();
            cluster.run(60_000);

            for (int observer = 0; observer < 5; observer++) {
                assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(observer, 4),
                        "node " + observer + " should have learned about the late joiner");
            }
        }
    }

    // ------------------------------------------------------------ failure detection

    @Test
    void detectsACrashedNode() {
        try (Cluster cluster = new Cluster(4)) {
            cluster.startAll();
            cluster.run(20_000);

            cluster.network.crash(cluster.id(3));
            cluster.run(60_000);

            for (int observer = 0; observer < 3; observer++) {
                assertEquals(Optional.of(MemberState.DEAD), cluster.stateOf(observer, 3),
                        "node " + observer + " should have buried the crashed node");
            }
        }
    }

    @Test
    void reportsFailureThroughListeners() {
        try (Cluster cluster = new Cluster(4)) {
            List<String> events = new ArrayList<>();
            cluster.node(0).addListener(new seanced.membership.MembershipListener() {
                @Override
                public void onSuspect(Member member) {
                    if (member.id().equals(cluster.id(3))) {
                        events.add("suspect");
                    }
                }

                @Override
                public void onDead(Member member) {
                    if (member.id().equals(cluster.id(3))) {
                        events.add("dead");
                    }
                }
            });

            cluster.startAll();
            cluster.run(20_000);
            cluster.network.crash(cluster.id(3));
            cluster.run(60_000);

            assertEquals(List.of("suspect", "dead"), events,
                    "a failure should be reported as suspicion first, then death");
        }
    }

    @Test
    void healthyNodesAreNeverFalselyDeclaredDead() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.startAll();
            cluster.run(120_000);

            for (int observer = 0; observer < 5; observer++) {
                for (int subject = 0; subject < 5; subject++) {
                    assertNotEquals(Optional.of(MemberState.DEAD), cluster.stateOf(observer, subject),
                            "no node should be declared dead on a healthy network");
                }
            }
        }
    }

    // ------------------------------------------------------------ indirect probing

    @Test
    void indirectProbesRescueANodeBehindABrokenLink() {
        try (Cluster cluster = new Cluster(4)) {
            cluster.startAll();
            cluster.run(20_000);

            // Nodes 0 and 1 cannot talk to each other, but both can reach 2 and 3.
            // Without indirect probing each would bury the other; with it, the helpers'
            // paths prove liveness and both stay alive.
            cluster.network.blockLink(cluster.id(0), cluster.id(1));
            cluster.run(60_000);

            assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(0, 1),
                    "node 1 is reachable via helpers and must not be buried");
            assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(1, 0),
                    "node 0 is reachable via helpers and must not be buried");
        }
    }

    @Test
    void withoutHelpersABrokenLinkDoesCauseAFalsePositive() {
        // The control for the test above: with only two nodes there is nobody to ask,
        // so the same broken link is indistinguishable from a crash. This is what the
        // indirect probe buys, and why k should never be configured to zero.
        try (Cluster cluster = new Cluster(2)) {
            cluster.startAll();
            cluster.run(20_000);

            cluster.network.blockLink(cluster.id(0), cluster.id(1));
            cluster.run(60_000);

            assertEquals(Optional.of(MemberState.DEAD), cluster.stateOf(0, 1));
        }
    }

    // ------------------------------------------------------------ refutation

    @Test
    void aSuspectedNodeRefutesAndStaysAlive() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.startAll();
            cluster.run(20_000);

            // Inject a false suspicion about node 1 into node 0, as a third party would.
            NodeId injector = new NodeId("10.9.9.9", 7946);
            Transport injectorTransport = cluster.network.join(injector);
            injectorTransport.send(cluster.id(0),
                    new Ping(injector, 1, List.of(MembershipUpdate.suspect(cluster.id(1), 0))));

            cluster.run(60_000);

            assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(0, 1),
                    "node 1 should have refuted the false suspicion");
            assertTrue(cluster.node(1).incarnation() > 0,
                    "refuting requires raising the incarnation above the rumour's");
        }
    }

    @Test
    void refutationOutrunsTheSuspicionAcrossTheCluster() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.startAll();
            cluster.run(20_000);

            NodeId injector = new NodeId("10.9.9.9", 7946);
            Transport injectorTransport = cluster.network.join(injector);
            injectorTransport.send(cluster.id(0),
                    new Ping(injector, 1, List.of(MembershipUpdate.suspect(cluster.id(2), 0))));

            cluster.run(90_000);

            for (int observer = 0; observer < 5; observer++) {
                if (observer == 2) {
                    continue;
                }
                assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(observer, 2),
                        "node " + observer + " should have seen the refutation");
            }
        }
    }

    // ------------------------------------------------------------ partitions

    @Test
    void eachSideOfAPartitionBuriesTheOther() {
        try (Cluster cluster = new Cluster(4)) {
            cluster.startAll();
            cluster.run(20_000);

            cluster.network.partition(Set.of(cluster.id(0), cluster.id(1)));
            cluster.run(90_000);

            // Within a side, everyone stays alive.
            assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(0, 1));
            assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(2, 3));
            // Across the split, each side concludes the other is gone. SWIM detects
            // unreachability; it cannot distinguish a partition from a mass failure.
            assertEquals(Optional.of(MemberState.DEAD), cluster.stateOf(0, 2));
            assertEquals(Optional.of(MemberState.DEAD), cluster.stateOf(2, 0));
        }
    }

    @Test
    void aBriefPartitionHealsWithoutAnyoneDying() {
        try (Cluster cluster = new Cluster(4)) {
            cluster.startAll();
            cluster.run(20_000);

            // Shorter than the suspicion timeout: nodes may be suspected, but the
            // refutation window has not closed, so nobody should be buried.
            cluster.network.partition(Set.of(cluster.id(0), cluster.id(1)));
            cluster.run(1_500);
            cluster.network.healPartition();
            cluster.run(60_000);

            for (int observer = 0; observer < 4; observer++) {
                for (int subject = 0; subject < 4; subject++) {
                    assertEquals(Optional.of(MemberState.ALIVE), cluster.stateOf(observer, subject),
                            "node " + observer + " should see node " + subject + " recovered");
                }
            }
        }
    }

    // ------------------------------------------------------------ lossy networks

    @Test
    void convergesDespiteHeavyPacketLoss() {
        // The suspicion timeout is raised well above the default here, and that is the
        // point rather than a workaround: it is the knob that trades detection latency
        // for false positives. At 30% loss a probe round fails often enough that a node
        // needs many periods of grace to get its refutation through. Leaving the timeout
        // at three periods on a network this lossy would bury healthy nodes — correctly
        // implemented, but misconfigured.
        SwimConfig lossy = SwimConfig.forTesting().toBuilder()
                .suspicionTimeoutMillis(15_000)
                .build();

        try (Cluster cluster = new Cluster(5, lossy)) {
            cluster.network.setLossRate(0.30);
            cluster.network.setRandom(new Random(99));

            cluster.startAll();
            cluster.run(200_000);

            for (int observer = 0; observer < 5; observer++) {
                for (int subject = 0; subject < 5; subject++) {
                    assertNotEquals(Optional.of(MemberState.DEAD), cluster.stateOf(observer, subject),
                            "30% loss should not bury anyone — that is what indirect probes are for");
                }
                // Membership, not liveness: under this much loss a peer may well be
                // SUSPECT at any given instant. That is the protocol working — suspicion
                // is transient and gets refuted. What must not happen is a burial.
                assertEquals(5, cluster.node(observer).membership().size(),
                        "node " + observer + " should still have found everyone");
            }
        }
    }

    // ------------------------------------------------------------ graceful leave

    @Test
    void leavingIsFasterThanBeingDetected() {
        try (Cluster cluster = new Cluster(4)) {
            cluster.startAll();
            cluster.run(20_000);

            // leave() stops the node probing and answering, so it goes silent exactly as
            // a crash would — the only difference is the farewell it sends on the way out.
            cluster.node(3).leave();

            // Well under the suspicion timeout: only the farewell can explain this.
            cluster.run(CONFIG.suspicionTimeoutMillis() / 2);

            assertEquals(Optional.of(MemberState.DEAD), cluster.stateOf(0, 3),
                    "a graceful leave should remove a node without waiting out a suspicion");
        }
    }

    // ------------------------------------------------------------ housekeeping

    @Test
    void deadMembersAreEventuallyForgotten() {
        SwimConfig config = SwimConfig.forTesting().toBuilder().deadReapMillis(30_000).build();
        try (Cluster cluster = new Cluster(4, config)) {
            cluster.startAll();
            cluster.run(20_000);

            cluster.network.crash(cluster.id(3));

            // Long enough to bury the node, short enough that the reaper has not yet run.
            cluster.run(15_000);
            assertEquals(Optional.of(MemberState.DEAD), cluster.stateOf(0, 3));

            cluster.run(60_000);

            assertTrue(cluster.node(0).membership().get(cluster.id(3)).isEmpty(),
                    "dead entries must be reaped or a churning cluster grows without bound");
        }
    }

    @Test
    void aSingleNodeClusterRunsQuietly() {
        try (Cluster cluster = new Cluster(1)) {
            cluster.startAll();
            cluster.run(60_000);

            assertEquals(1, cluster.node(0).membership().size());
            assertEquals(0, cluster.network.sentCount(), "with no peers there is nothing to probe");
        }
    }

    @Test
    void messageLoadStaysFlatAsTheClusterGrows() {
        // SWIM's headline property: each node sends a constant number of messages per
        // period regardless of cluster size, so total traffic is linear in N rather than
        // quadratic. Measured per node so the comparison is meaningful.
        double smallPerNode = messagesPerNode(3);
        double largePerNode = messagesPerNode(12);

        assertTrue(largePerNode < smallPerNode * 2.0,
                "per-node load should stay roughly flat: " + smallPerNode + " -> " + largePerNode);
    }

    private static double messagesPerNode(int size) {
        try (Cluster cluster = new Cluster(size)) {
            cluster.startAll();
            cluster.run(30_000);
            return (double) cluster.network.sentCount() / size;
        }
    }

    @Test
    void aCrashedNodeStopsBeingProbed() {
        try (Cluster cluster = new Cluster(4)) {
            cluster.startAll();
            cluster.run(20_000);

            cluster.network.crash(cluster.id(3));
            cluster.run(60_000);

            int afterBurial = cluster.network.sentCount();
            cluster.run(30_000);
            int laterStill = cluster.network.sentCount();

            // Traffic continues among the survivors, but the dead node is out of rotation,
            // so the rate should not include probes aimed at it.
            assertTrue(laterStill > afterBurial, "the surviving cluster keeps running");
            assertFalse(cluster.node(0).membership().alive().stream()
                            .anyMatch(member -> member.id().equals(cluster.id(3))),
                    "a dead node must not be selected as a probe target");
        }
    }
}
