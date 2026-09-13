package seanced.membership;

import org.junit.jupiter.api.Test;
import seanced.proto.MemberState;
import seanced.proto.MembershipUpdate;
import seanced.proto.NodeId;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MembershipListTest {

    private static final NodeId SELF = new NodeId("10.0.0.1", 7946);
    private static final NodeId PEER = new NodeId("10.0.0.2", 7946);
    private static final NodeId OTHER = new NodeId("10.0.0.3", 7946);

    private MembershipList newList() {
        return new MembershipList(SELF);
    }

    // ------------------------------------------------------------ basics

    @Test
    void startsKnowingOnlyItself() {
        MembershipList members = newList();

        assertEquals(1, members.size());
        assertEquals(MemberState.ALIVE, members.get(SELF).orElseThrow().state());
    }

    @Test
    void learnsUnknownNodes() {
        MembershipList members = newList();

        assertTrue(members.apply(MembershipUpdate.alive(PEER, 0)), "a new node is news");
        assertEquals(MemberState.ALIVE, members.get(PEER).orElseThrow().state());
    }

    @Test
    void ignoresUpdatesAboutItself() {
        MembershipList members = newList();

        assertFalse(members.apply(MembershipUpdate.dead(SELF, 99)),
                "only the node itself may change its own entry");
        assertEquals(MemberState.ALIVE, members.get(SELF).orElseThrow().state());
    }

    // ------------------------------------------------------------ merge precedence

    @Test
    void aliveOverridesSuspectOnlyAtAHigherIncarnation() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 5));
        members.apply(MembershipUpdate.suspect(PEER, 5));

        assertFalse(members.apply(MembershipUpdate.alive(PEER, 5)),
                "matching the suspicion is not enough — a node must out-run it");
        assertEquals(MemberState.SUSPECT, members.get(PEER).orElseThrow().state());

        assertTrue(members.apply(MembershipUpdate.alive(PEER, 6)), "a refutation wins");
        assertEquals(MemberState.ALIVE, members.get(PEER).orElseThrow().state());
    }

    @Test
    void suspectOverridesAliveAtTheSameIncarnation() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 3));

        assertTrue(members.apply(MembershipUpdate.suspect(PEER, 3)),
                "a failed probe should start the clock without winning a race first");
        assertEquals(MemberState.SUSPECT, members.get(PEER).orElseThrow().state());
    }

    @Test
    void suspectDoesNotOverrideAnEqualSuspicion() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 3));
        members.apply(MembershipUpdate.suspect(PEER, 3));

        assertFalse(members.apply(MembershipUpdate.suspect(PEER, 3)),
                "re-hearing the same suspicion is not news and must not be re-gossiped");
    }

    @Test
    void staleAliveIsIgnored() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 7));

        assertFalse(members.apply(MembershipUpdate.alive(PEER, 3)));
        assertEquals(7, members.get(PEER).orElseThrow().incarnation());
    }

    @Test
    void deadOverridesAliveAtTheSameIncarnation() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 100));

        assertTrue(members.apply(MembershipUpdate.dead(PEER, 100)),
                "a confirmed failure needs no incarnation bump to take effect");
        assertEquals(MemberState.DEAD, members.get(PEER).orElseThrow().state());
    }

    @Test
    void deadOverridesSuspect() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 2));
        members.apply(MembershipUpdate.suspect(PEER, 2));

        assertTrue(members.apply(MembershipUpdate.dead(PEER, 2)));
        assertEquals(MemberState.DEAD, members.get(PEER).orElseThrow().state());
    }

    @Test
    void staleNewsDoesNotResurrectADeadNode() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.dead(PEER, 5));

        assertFalse(members.apply(MembershipUpdate.alive(PEER, 5)),
                "an ALIVE rumour from before the death is stale and must not undo it");
        assertFalse(members.apply(MembershipUpdate.suspect(PEER, 999)),
                "a suspicion is weaker news than a death and never overturns one");
        assertEquals(MemberState.DEAD, members.get(PEER).orElseThrow().state());
    }

    @Test
    void aLiveNodeCanRefuteItsOwnDeath() {
        // Only the node itself raises its incarnation, so in practice a genuinely dead
        // node stays dead. But a live node wrongly buried during a partition must be able
        // to come back — otherwise one side of a healed partition permanently poisons the
        // other, since DEAD would outrank everything and spread back on reconnection.
        MembershipList members = newList();
        members.apply(MembershipUpdate.dead(PEER, 1));

        assertTrue(members.apply(MembershipUpdate.alive(PEER, 2)));
        assertEquals(MemberState.ALIVE, members.get(PEER).orElseThrow().state());
    }

    @Test
    void staleDeathIsIgnoredAfterARefutation() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 4));

        assertFalse(members.apply(MembershipUpdate.dead(PEER, 3)),
                "a death claim about an older incarnation has already been answered");
        assertEquals(MemberState.ALIVE, members.get(PEER).orElseThrow().state());
    }

    // ------------------------------------------------------------ listeners

    @Test
    void notifiesListenersOfLifecycleTransitions() {
        MembershipList members = newList();
        List<String> events = new ArrayList<>();
        members.addListener(new MembershipListener() {
            @Override
            public void onJoin(Member member) {
                events.add("join");
            }

            @Override
            public void onSuspect(Member member) {
                events.add("suspect");
            }

            @Override
            public void onDead(Member member) {
                events.add("dead");
            }

            @Override
            public void onRecover(Member member) {
                events.add("recover");
            }
        });

        members.apply(MembershipUpdate.alive(PEER, 0));
        members.apply(MembershipUpdate.suspect(PEER, 0));
        members.apply(MembershipUpdate.alive(PEER, 1));
        members.apply(MembershipUpdate.suspect(PEER, 1));
        members.apply(MembershipUpdate.dead(PEER, 1));

        assertEquals(List.of("join", "suspect", "recover", "suspect", "dead"), events);
    }

    @Test
    void aThrowingListenerDoesNotBreakTheTable() {
        MembershipList members = newList();
        members.addListener(new MembershipListener() {
            @Override
            public void onJoin(Member member) {
                throw new IllegalStateException("listener blew up");
            }
        });

        assertTrue(members.apply(MembershipUpdate.alive(PEER, 0)));
        assertEquals(MemberState.ALIVE, members.get(PEER).orElseThrow().state());
    }

    // ------------------------------------------------------------ selection

    @Test
    void probesEveryPeerOnceBeforeRepeatingAny() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 0));
        members.apply(MembershipUpdate.alive(OTHER, 0));

        Random random = new Random(1);
        List<NodeId> firstRound = List.of(
                members.nextProbeTarget(random).orElseThrow(),
                members.nextProbeTarget(random).orElseThrow());

        assertEquals(Set.of(PEER, OTHER), Set.copyOf(firstRound),
                "round-robin over a shuffle bounds how long any node can go unprobed");
    }

    @Test
    void neverProbesItself() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 0));

        Random random = new Random(7);
        for (int i = 0; i < 20; i++) {
            assertEquals(PEER, members.nextProbeTarget(random).orElseThrow());
        }
    }

    @Test
    void neverProbesDeadNodes() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 0));
        members.apply(MembershipUpdate.alive(OTHER, 0));
        members.apply(MembershipUpdate.dead(OTHER, 0));

        Random random = new Random(3);
        for (int i = 0; i < 20; i++) {
            assertEquals(PEER, members.nextProbeTarget(random).orElseThrow());
        }
    }

    @Test
    void hasNoProbeTargetWhenAloneInTheCluster() {
        assertTrue(newList().nextProbeTarget(new Random(0)).isEmpty());
    }

    @Test
    void picksHelpersFromLivePeersOnly() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 0));
        members.apply(MembershipUpdate.alive(OTHER, 0));
        members.apply(MembershipUpdate.suspect(OTHER, 0));

        List<NodeId> helpers = members.randomAlivePeers(5, new Random(0), Set.of());

        assertEquals(List.of(PEER), helpers, "a suspected node is a poor choice of helper");
    }

    @Test
    void honoursTheHelperExclusionList() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 0));
        members.apply(MembershipUpdate.alive(OTHER, 0));

        List<NodeId> helpers = members.randomAlivePeers(5, new Random(0), Set.of(PEER));

        assertEquals(List.of(OTHER), helpers);
    }

    // ------------------------------------------------------------ anti-entropy

    @Test
    void antiEntropySampleCyclesThroughMembers() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 0));
        members.apply(MembershipUpdate.alive(OTHER, 0));

        Set<NodeId> seen = new java.util.HashSet<>();
        for (int i = 0; i < 6; i++) {
            members.antiEntropySample(1, Set.of()).forEach(update -> seen.add(update.node()));
        }

        assertEquals(Set.of(SELF, PEER, OTHER), seen,
                "repeated samples should cover the table, not repeat one entry");
    }

    @Test
    void antiEntropySampleRespectsExclusions() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 0));

        List<MembershipUpdate> sample = members.antiEntropySample(10, Set.of(SELF, PEER));

        assertTrue(sample.isEmpty());
    }

    @Test
    void removeForgetsAPeerButNeverSelf() {
        MembershipList members = newList();
        members.apply(MembershipUpdate.alive(PEER, 0));

        members.remove(PEER);
        members.remove(SELF);

        assertTrue(members.get(PEER).isEmpty());
        assertTrue(members.get(SELF).isPresent(), "a node must always know about itself");
    }
}
