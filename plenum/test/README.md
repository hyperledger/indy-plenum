To test, use pytest. In PyCharm, this is easy to do by going into 
File -> Settings -> Tools -> Python Integrated Tools, and changing 
the "Default test runner" to "py.test".

To test from the command line: 

```
python3 -m pytest
```

Jason recommends Grep Console if using PyCharm

Because we have PortDispenser, our tests won't trip over each others 
ports. This means we can run tests in parallel, even when they are run from 
different python interpreters. (Hint, we're using good (arguable) old-fashioned 
(no question) file locking as a system-wide semaphore.)

To speed up tests, Jason recommends using the xdist plugin: 

https://pytest.org/latest/xdist.html


# TODO poll for connection state for now, but a comment to go back and add event publishing
# joins can be allowed either way, but roles can control that
# TODO establish list of faults (malicious and otherwise) and build tests for these
# TODO tests for adding and removing servers from the pool
# TODO Benign faults
"""
1. Replicas only accept PRE-PREPARE, PREPARE and COMMIT message when the replica is in the same view number as these
 requests contain. But then, what happens to requests that are in PRE-PREPARE, PREPARE or COMMIT phase and the view
 changes? Now the replicas wont execute this request because the will have a different view number since the view
 has changed

2. A non primary replica `R` gets PRE_PREPARE request for a client request before it has got the client request
from node for ordering
"""

# TODO Malign faults
"""
1. Primary assigns the same PRE-PREPARE sequence number to different requests
2. Primary stops assigning PRE-PREPARE sequence number to requests
3. Primary leaves gaps between PRE-PREPARE sequence numbers
4. Primary sends same pre-prepare request multiple times to non primary replicas to get multiple prepare requests
    from them for the same request
    In the above 4 cases non primary replicas should detect the malign behavior and initiate a view change
4. Primary replica drops client request or delays them.
"""


    """

""" RBFT Doc Scenarios

The following is taken from the RBFT whitepaper. I propose we use
these as specifications and build tests directly against them.

RBFT requires 3f+1 nodes (i.e., 3f + 1 physical machines).
Each node runs f + 1 protocol instances of a BFT protocol in
parallel. (f + 1 protocol instances is necessary and sufficient
to detect a faulty primary and ensure the robustness of the
protocol. This means that each of the N nodes in the system runs
locally one replica for each protocol instance.

Note that the different instances order
the requests following a 3-phase commit protocol similar to
PBFT [4].

Primary replicas of the various instances are placed
on nodes in such a way that, at any time, there is at most one
primary replica per node.

One of the f + 1 protocol instances
is called the master instance, while the others are called the
backup instances. All instances order client requests, but only
the requests ordered by the master instance are executed by
the nodes.

Backup instances only order requests in order to
be able to monitor the master instance. For that purpose, each
node runs a monitoring module that computes the throughput
of the f +1 protocol instances.

If n-f nodes observe that the
ratio between the performance of the master instance and the
best backup instance is lower than a given threshold, then the
primary of the master instance is considered to be malicious,
and a new one is elected. Intuitively, this means that a majority
of correct nodes agree on the fact that a protocol instance
change is necessary (the full correctness proof of RBFT can
be found in Section VI).

An alternative could be to changeIn RBFT, the high level goal is the same as for the other
robust BFT protocols we have studied previously: replicas
monitor the throughput of the primary and trigger the recovery
mechanism when the primary is slow. The approach we use
is radically different. It is not possible for replicas to guess
what the throughput of a non-malicious primary would be.
Therefore, the key idea is to leverage multicore architectures to
run multiple instances of the same protocol in parallel.
the master instance to the instance which provides the highest
throughput. This would require a mechanism to synchronize
the state of the different instances when switching, similar to
the switching mechanism of Abstract [9].



Nodes have to compare the throughput achieved by the different
instances to know whether a protocol instance change is
required or not. We are confident to say (given the theoretical
and experimental analysis) that this approach allows us to build
an effectively robust BFT protocol.

For RBFT to correctly work it is important that the f + 1
instances receive the same client requests. To that purpose
when a node receives a client request, it does not give it
directly to the f + 1 replicas it is hosting. Rather, it forwards
the request to all other nodes.

When a node has received f + 1
copies of a client request (possibly including its own copy),
it knows that every correct node will eventually receive the
request (because the request has been sent to at least one
correct node). Consequently, it gives the request to the f + 1
replicas it hosts.

Finally, note that each protocol instance implements a full-
fledged BFT protocol, very similar to the Aardvark protocol
described in the previous section.

There is nevertheless a
significant difference: a protocol instance does not proceed
to a view change by its own. Indeed, the view changes in
RBFT are controlled by the monitoring mechanism and apply
on every protocol instance at the same time.

message m signed by node i’s public key by <m>σi
message m authenticated by node i with a MAC for a node j by <m>µi,j
message m authenticated by node i with a MAC authenticator, i.e., an array containing one MAC per node, by <m>~µi


DETAILED PROTOCOL STEPS
STEP 1 ===The client sends a request to all the nodes.===
A client
c sends a REQUEST message <<REQUEST, o, rid, c>σc, c>~µc
to all the nodes (Step 1 in the figure). This message contains the
requested operation o, a request identifier rid, and the client
id c. It is signed with c’s private key, and then authenticated
with a MAC authenticator for all nodes.

On reception of a
REQUEST message, a node i verifies the MAC authenticator.

If the MAC is valid, it verifies the signature of the request. If
the signature is invalid, then the client is blacklisted: further
requests will not be processed.

If the request has already been executed, i resends the reply to the client. Otherwise, it moves
to the following step.

We use signatures as the request needs
to be forwarded by nodes to each other and using a MAC
authenticator alone would fail in guaranteeing non-repudiation,
as detailed in [6].

Similarly, using signatures alone would
open the possibility for malicious clients to overload nodes
by sending unfaithful requests with wrong signatures, that are
more costly to verify than MACs.

STEP 2 ===The correct nodes propagate the request to all the nodes.===

Once the request has been verified, the node i sends a
<PROPAGATE,<REQUEST, o, s, c>σc, i>~µi message to all nodes.
This step ensures that every correct node will eventually
receive the request as long as the request has been sent to
at least one correct node.

On reception of a PROPAGATE
message coming from node j, node i first verifies the MAC
authenticator.

If the MAC is valid, and it is the first time i
receives this request, i verifies the signature of the request.

If the signature is valid, i sends a PROPAGATE message to
the other nodes.

When a node receives f + 1 PROPAGATE
messages for a given request, the request is ready to be given
to the replicas of the f + 1 protocol instances running locally,
for ordering.

As proved in Section VI, only f + 1 PROPAGATE
messages are sufficient to guarantee that if malicious primaries
can order a given request, all correct primaries will eventually
be able to order the same request. Note that the replicas do
not order the whole request but only its identifiers (i.e., the
client id, request id and digest). Not only the whole request is
not necessary for the ordering phase, but it also improves the
performance as there is less data to process.

STEPS 3. 4. and 5. ===The replicas of each protocol instance execute
    a three phase commit protocol to order the request.===

When the primary replica p of a protocol instance receives a request,
it sends a PRE-PREPARE message <PRE-PREPARE, v, n, c, rid, d>~µp
authenticated with a MAC authenticator for every
replica of its protocol instance (Step 3 in the figure).

A replica that is not the primary of its protocol instance stores the
message and expects a corresponding PRE-PREPARE message.

When a replica receives a PRE-PREPARE message from the
primary of its protocol instance, it verifies the validity of
the MAC.

It then replies to the PRE-PREPARE message by
sending a PREPARE message to all other replicas, only if the
node it is running on already received f+1 copies of the
request. Without this verification, a malicious primary may
collude with faulty clients that would send correct requests
only to him, in order to boost the performance of the protocol
instance of the malicious primary at the expense of the other
protocol instances.

Following the reception of n-f-1 matching
PREPARE messages from distinct replicas of the same protocol
instance that are consistent with a PRE-PREPARE message,
a replica r sends a commit message <COMMIT, v, n, d, r>~µr
that is authenticated with a MAC authenticator (Step 5 in
the figure).

After the reception of n-f matching COMMIT
messages from distinct replicas of the same protocol instance,
a replica gives back the ordered request to the node it is
running on.

STEP 6. ===The nodes execute the request and send a reply message
to the client.===

Each time a node receives an ordered request
from a replica of the master instance, the request operation
is executed.

After the operation has been executed, the node
i sends a REPLY message <REPLY, u, i>µi,c to client c that is
authenticated with a MAC, where u is the result of the request
execution (Step 6 in the figure).

When the client c receives f+1 valid and matching
<REPLY, u, i>µi,c from different nodes i, it accepts u as the
result of the execution of the request.

===C. Monitoring mechanism specs here===

RBFT implements a monitoring mechanism to detect
whether the master protocol instance is faulty or not. This
monitoring mechanism works as follows. Each node keeps
a counter nbreqs-i for each protocol instance i, which cor-
responds to the number of requests that have been ordered
by the replica of the corresponding instance (i.e. for which
n-f COMMIT messages have been collected).

Periodically,
the node uses these counters to compute the throughput of
each protocol instance replica and then resets the counters.

The throughput values are compared as follows. If the ratio
between the throughput of the master instance t-master and the
average throughput of the backup instances t-backup is lower
than a given threshold ∆, then the primary of the master
protocol instance is suspected to be malicious, and the node
initiates a protocol instance change, as detailed in the next
section.

The value of ∆ depends on the ratio between the
throughput observed in the fault-free case and the throughput
observed under attack.

Note that the state of the different
protocol instances is not synchronized: they can diverge and
order requests in different orders without compromising the
safety nor the liveness of the protocol.

In addition to the monitoring of the throughput, the moni-
toring mechanism also tracks the time needed by the replicas
to order the requests.

This mechanism ensures that the primary
of the master protocol instance is fair towards all the clients.
Specifically, each node measures the individual latency of
each request, say lat-req and the average latency for each
client, say lat-c , for each replica running on the same node.
A configuration parameter, Λ, defines the maximal acceptable
latency for any given request. Another parameter, Ω, defines
the maximal acceptable difference between the average latency
of a client on the different protocol instances. These two
parameters depend on the workload and on the experimental
settings. When the node sends a request to the various replicas
running locally for ordering, it records the current time.
Then, when it receives the corresponding ordered request, it
computes its latency lat req and the average latency for all the
requests of this client lat c . If this request has been ordered by
the replica of the master instance and if lat req is greater than
Λ, or the difference between lat c and the average latency for
this client on the other protocol instances is greater than Ω,
then the node starts a protocol instance change. Note that we
define the value of the different parameters, ∆, Λ and Ω both
theoretically and experimentally, as described in Section VI:
their value depends on the cost of the cryptographic operations
and on the network conditions.

===D. Protocol instance change mechanism===

In this section, we describe the protocol instance change
mechanism that is used to replace the faulty primary at the
master protocol instance. Because there is only at most one
primary per node in RBFT, this also implies to replace all the
primaries on all the protocol instances.

Each node i keeps a counter cpi i , which uniquely iden-
tifies a protocol instance change message.

When a node i detects too much difference between the performance of
the master instance and the performance of the backup in-
stances (as detailed in the previous section), it sends an
<INSTANCE CHANGE, cpi, i>~μi message authenticated with a
MAC authenticator to all the other nodes.

When a node j receives an INSTANCE CHANGE message
from node i, it verifies the MAC and handles it as follows.

If cpi i < cpi j , then this message was intended for a previous
INSTANCE CHANGE and is discarded.

On the contrary, if
cpi i ≥ cpi j , then the node checks if it should also send
an INSTANCE CHANGE message. It does so only if it also
observes too much difference between the performance of the
replicas.

Upon the reception of n-f valid and matching
INSTANCE CHANGE messages, the node increments cpi and
initiates a view change on every protocol instance that runs
locally.

As a result, each protocol instance elects a new
primary and the malicious replica of the master instance is
no longer the primary.

    """


