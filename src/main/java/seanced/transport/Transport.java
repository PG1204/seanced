package seanced.transport;

import seanced.proto.Message;
import seanced.proto.NodeId;

import java.util.function.Consumer;

/**
 * How a node sends and receives protocol messages.
 *
 * <p>The second seam in the system, after {@code Clock}. Abstracting the network
 * lets the whole protocol run against an in-memory implementation that can drop,
 * delay and partition traffic on command — the conditions SWIM exists to survive,
 * and ones you cannot reliably produce against a real socket.
 *
 * <p>{@link #send} is deliberately fire-and-forget, with no return value and no
 * exception on failure. SWIM assumes an unreliable datagram network; a transport
 * that reported delivery would tempt callers into relying on a guarantee the
 * protocol is designed not to need.
 */
public interface Transport extends AutoCloseable {

    /** Best-effort send. Messages may be dropped, duplicated, or reordered. */
    void send(NodeId to, Message message);

    /**
     * Registers the handler for inbound messages. Replaces any previous handler.
     * Must be called before messages can be received.
     */
    void listen(Consumer<Message> handler);

    /** This node's own address, as peers should address it. */
    NodeId localAddress();

    @Override
    void close();
}
