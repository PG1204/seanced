package seanced.gossip;

import org.junit.jupiter.api.Test;
import seanced.proto.MemberState;
import seanced.proto.MembershipUpdate;
import seanced.proto.NodeId;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GossipQueueTest {

    private static final NodeId A = new NodeId("10.0.0.1", 7946);
    private static final NodeId B = new NodeId("10.0.0.2", 7946);

    @Test
    void drainsQueuedRumors() {
        GossipQueue queue = new GossipQueue(4);
        queue.add(MembershipUpdate.alive(A, 1));

        assertEquals(List.of(MembershipUpdate.alive(A, 1)), queue.drain(10, 3));
    }

    @Test
    void honoursTheBatchLimit() {
        GossipQueue queue = new GossipQueue(4);
        for (int i = 1; i <= 5; i++) {
            queue.add(MembershipUpdate.alive(new NodeId("10.0.0." + i, 7946), 0));
        }

        assertEquals(2, queue.drain(2, 10).size(), "batches are capped to keep datagrams under the MTU");
    }

    @Test
    void newerNewsReplacesOlderNewsAboutTheSameNode() {
        GossipQueue queue = new GossipQueue(4);
        queue.add(MembershipUpdate.alive(A, 1));
        queue.add(MembershipUpdate.suspect(A, 1));

        List<MembershipUpdate> drained = queue.drain(10, 3);

        assertEquals(1, drained.size(), "the queue holds current state per node, not a history");
        assertEquals(MemberState.SUSPECT, drained.get(0).state());
    }

    @Test
    void retiresARumorAfterItsRetransmitLimit() {
        GossipQueue queue = new GossipQueue(4);
        queue.add(MembershipUpdate.alive(A, 1));

        int limit = queue.retransmitLimit(3);
        for (int i = 0; i < limit; i++) {
            assertFalse(queue.drain(10, 3).isEmpty(), "should still be spreading on send " + (i + 1));
        }

        assertTrue(queue.drain(10, 3).isEmpty(), "a well-spread rumour stops consuming bandwidth");
        assertTrue(queue.isEmpty());
    }

    @Test
    void freshRumorsAreNotStarvedByOlderOnes() {
        GossipQueue queue = new GossipQueue(6);
        queue.add(MembershipUpdate.alive(A, 1));

        // Spread A around a bit, then introduce fresher news about B.
        queue.drain(10, 5);
        queue.drain(10, 5);
        queue.add(MembershipUpdate.suspect(B, 1));

        List<MembershipUpdate> batch = queue.drain(1, 5);

        assertEquals(B, batch.get(0).node(), "least-transmitted goes first");
    }

    @Test
    void retransmitLimitGrowsLogarithmicallyWithClusterSize() {
        GossipQueue queue = new GossipQueue(4);

        int small = queue.retransmitLimit(3);
        int medium = queue.retransmitLimit(100);
        int large = queue.retransmitLimit(10_000);

        assertTrue(small < medium && medium < large, "more nodes need more rounds to reach");
        assertTrue(large < 10 * small, "but growth must stay logarithmic, not linear");
    }

    @Test
    void retransmitLimitIsAtLeastOneEvenForATrivialCluster() {
        GossipQueue queue = new GossipQueue(1);

        assertTrue(queue.retransmitLimit(0) >= 1);
        assertTrue(queue.retransmitLimit(1) >= 1);
    }

    @Test
    void emptyQueueDrainsToNothing() {
        assertTrue(new GossipQueue(4).drain(10, 5).isEmpty());
    }

    @Test
    void drainingZeroTakesNothingAndCostsNoTransmission() {
        GossipQueue queue = new GossipQueue(4);
        queue.add(MembershipUpdate.alive(A, 1));

        assertTrue(queue.drain(0, 5).isEmpty());
        assertEquals(1, queue.size(), "an un-sent rumour must not be counted as transmitted");
    }

    @Test
    void spreadsEveryRumorWhenBatchesAreSmallerThanTheQueue() {
        GossipQueue queue = new GossipQueue(4);
        Set<NodeId> queued = new HashSet<>();
        for (int i = 1; i <= 5; i++) {
            NodeId node = new NodeId("10.0.0." + i, 7946);
            queued.add(node);
            queue.add(MembershipUpdate.alive(node, 0));
        }

        Set<NodeId> seen = new HashSet<>();
        for (int round = 0; round < 20 && !queue.isEmpty(); round++) {
            queue.drain(1, 10).forEach(update -> seen.add(update.node()));
        }

        assertEquals(queued, seen, "no rumour should be starved out of the rotation");
    }
}
