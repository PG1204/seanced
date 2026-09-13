package seanced.codec;

import org.junit.jupiter.api.Test;
import seanced.proto.Ack;
import seanced.proto.MemberState;
import seanced.proto.MembershipUpdate;
import seanced.proto.Message;
import seanced.proto.NodeId;
import seanced.proto.Ping;
import seanced.proto.PingReq;
import seanced.proto.PingReqAck;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageCodecTest {

    private static final NodeId A = new NodeId("10.0.0.1", 7946);
    private static final NodeId B = new NodeId("10.0.0.2", 7946);

    private static Message roundTrip(Message message) {
        return MessageCodec.decode(MessageCodec.encode(message));
    }

    @Test
    void roundTripsPing() {
        Ping original = new Ping(A, 42, List.of());
        assertEquals(original, roundTrip(original));
    }

    @Test
    void roundTripsAck() {
        Ack original = new Ack(B, 7, List.of());
        assertEquals(original, roundTrip(original));
    }

    @Test
    void roundTripsPingReq() {
        PingReq original = new PingReq(A, B, 99, List.of());
        assertEquals(original, roundTrip(original));
    }

    @Test
    void roundTripsPingReqAckBothWays() {
        assertEquals(new PingReqAck(A, B, 1, true, List.of()),
                roundTrip(new PingReqAck(A, B, 1, true, List.of())));
        assertEquals(new PingReqAck(A, B, 1, false, List.of()),
                roundTrip(new PingReqAck(A, B, 1, false, List.of())));
    }

    @Test
    void roundTripsPiggybackedGossip() {
        List<MembershipUpdate> gossip = List.of(
                MembershipUpdate.alive(A, 3),
                MembershipUpdate.suspect(B, 1),
                MembershipUpdate.dead(new NodeId("10.0.0.3", 7946), 12));

        Message decoded = roundTrip(new Ping(A, 5, gossip));

        assertEquals(gossip, decoded.gossip());
    }

    @Test
    void preservesUnicodeHostnames() {
        NodeId unicode = new NodeId("höst-ü.example.com", 9999);
        Ping original = new Ping(unicode, 1, List.of(MembershipUpdate.alive(unicode, 0)));

        assertEquals(original, roundTrip(original));
    }

    @Test
    void preservesLargeSequenceNumbers() {
        Ping original = new Ping(A, Long.MAX_VALUE, List.of());
        assertEquals(Long.MAX_VALUE, roundTrip(original).seqNo());
    }

    @Test
    void staysUnderTheMtuWithAFullGossipLoad() {
        // The realistic worst case: a full gossip batch on top of the largest message type.
        List<MembershipUpdate> gossip = new java.util.ArrayList<>();
        for (int i = 0; i < 8; i++) {
            gossip.add(MembershipUpdate.alive(new NodeId("192.168.100." + i, 7946), i));
        }

        byte[] encoded = MessageCodec.encode(new PingReqAck(A, B, 1, true, gossip));

        assertTrue(encoded.length < MessageCodec.MAX_PACKET_BYTES,
                "encoded " + encoded.length + " bytes, must stay under the MTU budget");
    }

    @Test
    void rejectsGarbage() {
        assertThrows(MessageCodec.MalformedMessageException.class,
                () -> MessageCodec.decode(new byte[]{9, 9, 9, 9}));
    }

    @Test
    void rejectsTruncatedMessage() {
        byte[] encoded = MessageCodec.encode(new Ping(A, 1, List.of(MembershipUpdate.alive(B, 0))));
        byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length - 4);

        assertThrows(MessageCodec.MalformedMessageException.class,
                () -> MessageCodec.decode(truncated));
    }

    @Test
    void rejectsUnknownMemberStateCode() {
        assertThrows(IllegalArgumentException.class, () -> MemberState.fromCode((byte) 77));
    }

    @Test
    void memberStateCodesAreStable() {
        // These values are on the wire; changing them breaks compatibility with older nodes.
        assertEquals(0, MemberState.ALIVE.code());
        assertEquals(1, MemberState.SUSPECT.code());
        assertEquals(2, MemberState.DEAD.code());
    }
}
