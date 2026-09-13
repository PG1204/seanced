package seanced.app;

import seanced.SwimConfig;
import seanced.SwimNode;
import seanced.clock.SystemClock;
import seanced.membership.Member;
import seanced.membership.MembershipListener;
import seanced.proto.NodeId;
import seanced.transport.UdpTransport;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs a single cluster member.
 *
 * <pre>
 *   seanced --port 7946
 *   seanced --port 7947 --seed 127.0.0.1:7946
 * </pre>
 *
 * <p>Start one node with no seed, then point others at it. Seeds are only needed to
 * find a way in — gossip handles the rest, and the seed itself is not special once
 * the cluster is formed.
 */
public final class Main {

    public static void main(String[] args) {
        Options options;
        try {
            options = Options.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(Options.USAGE);
            System.exit(2);
            return;
        }

        NodeId self = new NodeId(options.host, options.port);

        SystemClock clock = new SystemClock();
        UdpTransport transport = new UdpTransport(self);
        SwimNode node = new SwimNode(self, SwimConfig.defaults(), clock, transport);

        node.addListener(new MembershipListener() {
            @Override
            public void onJoin(Member member) {
                System.out.println("[join]    " + member.id());
            }

            @Override
            public void onSuspect(Member member) {
                System.out.println("[suspect] " + member.id());
            }

            @Override
            public void onDead(Member member) {
                System.out.println("[dead]    " + member.id());
            }

            @Override
            public void onRecover(Member member) {
                System.out.println("[recover] " + member.id());
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Announce departure so peers drop us at once instead of waiting out a suspicion.
            node.leave();
            node.close();
            clock.close();
        }, "seanced-shutdown"));

        node.join(options.seeds.toArray(new NodeId[0]));
        node.start();

        System.out.println("seanced: node " + self + " listening"
                + (options.seeds.isEmpty() ? " (no seeds — starting a new cluster)"
                                           : ", joining via " + options.seeds));

        // The clock and transport run on daemon threads, so park the main thread here.
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record Options(String host, int port, List<NodeId> seeds) {

        static final String USAGE = """
                usage: seanced --port <port> [--host <host>] [--seed <host:port>]...

                  --port  port to listen on (required)
                  --host  address peers should use to reach this node (default 127.0.0.1)
                  --seed  an existing member to join through; may be repeated""";

        static Options parse(String[] args) {
            String host = "127.0.0.1";
            Integer port = null;
            List<NodeId> seeds = new ArrayList<>();

            for (int i = 0; i < args.length; i++) {
                String flag = args[i];
                String value = requireValue(args, i + 1, flag);
                switch (flag) {
                    case "--port" -> port = parsePort(value);
                    case "--host" -> host = value;
                    case "--seed" -> seeds.add(NodeId.parse(value));
                    default -> throw new IllegalArgumentException("unknown argument: " + flag);
                }
                i++;
            }

            if (port == null) {
                throw new IllegalArgumentException("--port is required");
            }
            return new Options(host, port, List.copyOf(seeds));
        }

        private static String requireValue(String[] args, int index, String flag) {
            if (index >= args.length) {
                throw new IllegalArgumentException(flag + " needs a value");
            }
            return args[index];
        }

        private static int parsePort(String value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("--port must be a number, got: " + value, e);
            }
        }
    }
}
