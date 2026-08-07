# seanced

A SWIM-based cluster membership and failure detection library in Java.

In a cluster of N nodes, each node needs to know which peers are alive
without every node constantly pinging every other node. seanced implements
the SWIM protocol: nodes detect failures through randomized probing (with
indirect checks to avoid false alarms) and spread membership changes to the
whole cluster via piggybacked gossip, so every node converges on a shared
view of who's alive.

