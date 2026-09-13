# seanced

A SWIM-based cluster membership and failure detection library in Java.

In a cluster of N nodes, each node needs to know which peers are alive
without every node constantly pinging every other node. seanced implements
the SWIM protocol: nodes detect failures through randomized probing (with
indirect checks to avoid false alarms) and spread membership changes to the
whole cluster via piggybacked gossip, so every node converges on a shared
view of who's alive.

Requires Java 21.

## Quick start

```java
NodeId self = new NodeId("10.0.0.1", 7946);
SystemClock clock = new SystemClock();
UdpTransport transport = new UdpTransport(self);

SwimNode node = new SwimNode(self, SwimConfig.defaults(), clock, transport);
node.addListener(new MembershipListener() {
    @Override public void onJoin(Member m) { System.out.println("joined: " + m.id()); }
    @Override public void onDead(Member m) { System.out.println("lost:   " + m.id()); }
});

node.join(NodeId.parse("10.0.0.2:7946"));   // any existing member
node.start();
```

Seeds are only a way in. Once a node has talked to any member, gossip carries
its arrival to the rest of the cluster, and the seed stops being special.

To run a node from the command line:

```
./gradlew installDist
build/install/seanced/bin/seanced --port 7946
build/install/seanced/bin/seanced --port 7947 --seed 127.0.0.1:7946
```

## How it works

Every protocol period (1s by default) each node picks one peer and probes it:

1. **Direct probe.** Send a `Ping`, wait for an `Ack`.
2. **Indirect probe.** On timeout, ask *k* peers to probe the target via
   `PingReq`. Their independent network paths are what separate "this link is
   bad" from "that node is gone" — without this step every dropped packet
   would be a false alarm.
3. **Suspicion.** If nothing answers by the end of the period, mark the target
   `SUSPECT` and gossip it.
4. **Confirmation.** If the target does not refute within the suspicion
   timeout, declare it `DEAD`.

A node hearing itself called suspect or dead **refutes**: it raises its own
incarnation number and gossips `ALIVE` at the new value. Since only a node
increments its own incarnation, a higher one is authoritative and overrides
the suspicion wherever it has spread. This is what makes a transient network
problem recoverable rather than fatal.

Membership updates never get their own broadcast. They ride piggybacked on
failure-detector traffic, which is what keeps each node's message load
constant as the cluster grows.

## Architecture

| Package | Role |
|---|---|
| `proto` | Wire types: `NodeId`, `MemberState`, `MembershipUpdate`, and the sealed `Message` hierarchy |
| `codec` | `MessageCodec` — binary encode/decode, sized to fit one datagram |
| `clock` | `Clock` seam; `SystemClock` (real) and `FakeClock` (deterministic simulation) |
| `transport` | `Transport` seam; `UdpTransport` (real) and `InMemoryNetwork` (loss, latency, partitions) |
| `membership` | `MembershipList` — the state machine and the update-precedence rules |
| `gossip` | `GossipQueue` — rumor buffer with bounded retransmission |
| root | `SwimConfig`, `SwimNode` — the failure detector itself |

Every source of nondeterminism — time, the network, random peer selection —
is injected. A whole cluster therefore runs reproducibly on a single thread,
so protocol tests exercise partitions and packet loss exactly rather than
hoping to catch them in a timing window.

## Update precedence

Conflicting claims arrive out of order and from multiple hops. Every node
must resolve them identically or the cluster never converges:

| Incoming | Overrides |
|---|---|
| `ALIVE(i)` | anything at incarnation `< i` |
| `SUSPECT(i)` | `ALIVE(j)` when `i >= j`; `SUSPECT(j)` when `i > j` |
| `DEAD(i)` | `ALIVE(j)` and `SUSPECT(j)` when `i >= j` |

Equality favours suspicion, so a single failed probe starts the clock without
having to win a race. Death is not formally final: a live node wrongly buried
during a partition can still refute with a higher incarnation. In practice a
genuinely dead node stays dead, because nothing else raises its incarnation —
but making death strictly terminal would let one side of a healed partition
permanently poison the other.

## Configuration

`SwimConfig.defaults()` targets a LAN and detects a crash in roughly six
seconds. The knobs that matter:

- **`protocolPeriodMillis`** — the dominant control. Halving it doubles both
  detection speed and traffic.
- **`suspicionTimeoutMillis`** — the main false-positive control. It must
  exceed the worst case for a refutation to propagate, which grows with
  cluster size and with packet loss.
- **`indirectProbeCount`** (*k*) — independent paths checked before suspecting.
  Never set it to zero; that is what a false positive is made of.

## Testing

```
./gradlew test
```

71 tests. The cluster tests in `SwimClusterTest` run whole clusters against a
simulated clock and network, covering convergence, crash detection, indirect
probe rescue, refutation, partitions and healing, 30% packet loss, graceful
leave, and the constant per-node message load that is SWIM's headline property.

## Status and limitations

The protocol is implemented and tested. Before production use, note:

- **No authentication or encryption.** Anyone who can send a UDP packet to a
  member can forge membership updates and evict healthy nodes. This is the
  most important gap.
- **No full-state anti-entropy.** Spare gossip capacity is topped up with
  current membership, which converges well in testing, but there is no
  periodic full state sync over TCP as production SWIM implementations have.
- **No node metadata.** Members carry an address and nothing else; there is no
  way to attach application data such as roles or tags.
- **Gossip is capped by MTU.** Very large clusters will want message splitting
  or a TCP fallback for oversized payloads.
