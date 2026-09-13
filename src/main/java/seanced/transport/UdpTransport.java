package seanced.transport;

import seanced.codec.MessageCodec;
import seanced.proto.Message;
import seanced.proto.NodeId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.function.Consumer;

/**
 * The production transport: one UDP socket, one receiver thread.
 *
 * <p>UDP rather than TCP because SWIM's whole design assumes unreliable delivery
 * and already compensates for it. Per-peer TCP connections would cost a file
 * descriptor and a handshake per member, and a connection failure would confuse
 * failure detection with transport state — exactly the coupling SWIM avoids.
 *
 * <p>Send failures are swallowed rather than thrown. The protocol treats an
 * unanswered probe as evidence, and an exception escaping into the failure
 * detector would be a worse signal than the silence it already knows how to
 * interpret.
 */
public final class UdpTransport implements Transport {

    private static final System.Logger LOG = System.getLogger(UdpTransport.class.getName());

    private final NodeId local;
    private final DatagramSocket socket;
    private final Thread receiver;

    private volatile Consumer<Message> handler = message -> {
    };
    private volatile boolean running = true;

    public UdpTransport(NodeId local) {
        this.local = local;
        try {
            this.socket = new DatagramSocket(new InetSocketAddress(local.port()));
        } catch (SocketException e) {
            throw new UncheckedIOException("could not bind UDP port " + local.port(), e);
        }
        this.receiver = new Thread(this::receiveLoop, "seanced-udp-" + local.port());
        this.receiver.setDaemon(true);
        this.receiver.start();
    }

    @Override
    public void send(NodeId to, Message message) {
        if (!running) {
            return;
        }
        try {
            byte[] payload = MessageCodec.encode(message);
            if (payload.length > MessageCodec.MAX_PACKET_BYTES) {
                // Dropping beats fragmenting: an over-MTU datagram is lost wholesale if any
                // fragment goes missing, so it would silently degrade the failure detector.
                LOG.log(System.Logger.Level.WARNING,
                        "dropping oversized packet to {0}: {1} bytes", to, payload.length);
                return;
            }
            InetAddress address = InetAddress.getByName(to.host());
            socket.send(new DatagramPacket(payload, payload.length, address, to.port()));
        } catch (UnknownHostException e) {
            LOG.log(System.Logger.Level.WARNING, "cannot resolve " + to, e);
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "send to " + to + " failed", e);
        }
    }

    @Override
    public void listen(Consumer<Message> handler) {
        this.handler = handler;
    }

    @Override
    public NodeId localAddress() {
        return local;
    }

    private void receiveLoop() {
        byte[] buffer = new byte[MessageCodec.MAX_PACKET_BYTES * 2];
        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                Message message = MessageCodec.decode(packet.getData(), packet.getOffset(), packet.getLength());
                handler.accept(message);
            } catch (MessageCodec.MalformedMessageException e) {
                // Garbage on the port, or something that isn't us. Drop and keep serving.
                LOG.log(System.Logger.Level.DEBUG, "dropping malformed packet", e);
            } catch (IOException e) {
                if (running) {
                    LOG.log(System.Logger.Level.WARNING, "receive failed", e);
                }
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.ERROR, "handler threw", e);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        socket.close();
        receiver.interrupt();
    }
}
