package seanced.codec;

import seanced.proto.Ack;
import seanced.proto.MemberState;
import seanced.proto.MembershipUpdate;
import seanced.proto.Message;
import seanced.proto.NodeId;
import seanced.proto.Ping;
import seanced.proto.PingReq;
import seanced.proto.PingReqAck;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns {@link Message}s into bytes and back.
 *
 * <p>Hand-rolled rather than reflective, for three reasons: the format stays
 * small enough to fit a UDP datagram, there is no dependency to keep current,
 * and a malformed packet from the network can't be coerced into constructing
 * arbitrary objects.
 *
 * <p>Wire format, all integers big-endian:
 * <pre>
 *   u8    message type
 *   node  from
 *   i64   seqNo
 *   node  target        (PING_REQ and PING_REQ_ACK only)
 *   u8    reached       (PING_REQ_ACK only)
 *   u16   gossip count
 *   {node, u8 state, i64 incarnation} * count
 *
 *   node := u16 host-length, host bytes (UTF-8), i32 port
 * </pre>
 */
public final class MessageCodec {

    /**
     * Conservative payload ceiling. Ethernet's 1500-byte MTU less IPv4 and UDP
     * headers leaves 1472; backing off to 1400 leaves room for tunnels and VPNs
     * that add their own encapsulation. Exceeding the path MTU means fragmentation,
     * and a fragmented datagram is lost entirely if any fragment is dropped.
     */
    public static final int MAX_PACKET_BYTES = 1400;

    private static final byte TYPE_PING = 1;
    private static final byte TYPE_ACK = 2;
    private static final byte TYPE_PING_REQ = 3;
    private static final byte TYPE_PING_REQ_ACK = 4;

    private MessageCodec() {
    }

    public static byte[] encode(Message message) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(typeOf(message));
            writeNode(out, message.from());
            out.writeLong(message.seqNo());

            switch (message) {
                case Ping ignored -> {
                }
                case Ack ignored -> {
                }
                case PingReq pingReq -> writeNode(out, pingReq.target());
                case PingReqAck pingReqAck -> {
                    writeNode(out, pingReqAck.target());
                    out.writeBoolean(pingReqAck.reached());
                }
            }

            List<MembershipUpdate> gossip = message.gossip();
            if (gossip.size() > 0xFFFF) {
                throw new IllegalArgumentException("too many gossip entries: " + gossip.size());
            }
            out.writeShort(gossip.size());
            for (MembershipUpdate update : gossip) {
                writeNode(out, update.node());
                out.writeByte(update.state().code());
                out.writeLong(update.incarnation());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("encoding to memory should not fail", e);
        }
        return bytes.toByteArray();
    }

    public static Message decode(byte[] data) {
        return decode(data, 0, data.length);
    }

    /**
     * @throws MalformedMessageException if the bytes are not a well-formed message.
     *         Callers should drop the packet and carry on — a peer sending garbage,
     *         or a port collision with unrelated traffic, must not take the node down.
     */
    public static Message decode(byte[] data, int offset, int length) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data, offset, length))) {
            byte type = in.readByte();
            NodeId from = readNode(in);
            long seqNo = in.readLong();

            NodeId target = null;
            boolean reached = false;
            if (type == TYPE_PING_REQ) {
                target = readNode(in);
            } else if (type == TYPE_PING_REQ_ACK) {
                target = readNode(in);
                reached = in.readBoolean();
            }

            int gossipCount = in.readUnsignedShort();
            List<MembershipUpdate> gossip = new ArrayList<>(Math.min(gossipCount, 64));
            for (int i = 0; i < gossipCount; i++) {
                NodeId node = readNode(in);
                MemberState state = MemberState.fromCode(in.readByte());
                long incarnation = in.readLong();
                gossip.add(new MembershipUpdate(node, state, incarnation));
            }

            return switch (type) {
                case TYPE_PING -> new Ping(from, seqNo, gossip);
                case TYPE_ACK -> new Ack(from, seqNo, gossip);
                case TYPE_PING_REQ -> new PingReq(from, target, seqNo, gossip);
                case TYPE_PING_REQ_ACK -> new PingReqAck(from, target, seqNo, reached, gossip);
                default -> throw new MalformedMessageException("unknown message type: " + type);
            };
        } catch (IOException | IllegalArgumentException | IndexOutOfBoundsException e) {
            throw new MalformedMessageException("could not decode message", e);
        }
    }

    private static byte typeOf(Message message) {
        return switch (message) {
            case Ping ignored -> TYPE_PING;
            case Ack ignored -> TYPE_ACK;
            case PingReq ignored -> TYPE_PING_REQ;
            case PingReqAck ignored -> TYPE_PING_REQ_ACK;
        };
    }

    private static void writeNode(DataOutputStream out, NodeId node) throws IOException {
        byte[] host = node.host().getBytes(StandardCharsets.UTF_8);
        if (host.length > 0xFFFF) {
            throw new IllegalArgumentException("host name too long: " + host.length + " bytes");
        }
        out.writeShort(host.length);
        out.write(host);
        out.writeInt(node.port());
    }

    private static NodeId readNode(DataInputStream in) throws IOException {
        int hostLength = in.readUnsignedShort();
        byte[] host = new byte[hostLength];
        in.readFully(host);
        int port = in.readInt();
        return new NodeId(new String(host, StandardCharsets.UTF_8), port);
    }

    /** Thrown when incoming bytes don't parse. Unchecked: the only sane response is to drop the packet. */
    public static final class MalformedMessageException extends RuntimeException {
        public MalformedMessageException(String message) {
            super(message);
        }

        public MalformedMessageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
