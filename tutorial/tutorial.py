from ioflo.base.consoling import getConsole

from zeno.client.client import Client
from zeno.common.looper import Looper
from zeno.server.node import Node
from zeno.server.client_authn import SimpleAuthNr
from zeno.test.malicious_behaviors_node import changesRequest, makeNodeFaulty

console = getConsole()
console.reinit(verbosity=console.Wordage.terse)

# The looper runs a message loop that the nodes run on
looper = Looper()

# Dictionary of Node names and their IP Addresses and port numbers.
nodeReg = {
    'Alpha': ('127.0.0.1', 7560),
    'Beta': ('127.0.0.1', 7562),
    'Gamma': ('127.0.0.1', 7564),
    'Delta': ('127.0.0.1', 7566)}

alpha = Node('Alpha', nodeReg, SimpleAuthNr())
alpha.start()
alpha.startKeySharing()
looper.add(alpha)

beta = Node('Beta', nodeReg, SimpleAuthNr())
beta.start()
beta.startKeySharing()
looper.add(beta)

gamma = Node('Gamma', nodeReg, SimpleAuthNr())
gamma.start()
gamma.startKeySharing()
looper.add(gamma)

delta = Node('Delta', nodeReg, SimpleAuthNr())
delta.start()
delta.startKeySharing()
looper.add(delta)
looper.runFor(30)

cliNodeReg = {
    'AlphaC': ('127.0.0.1', 7561),
    'BetaC': ('127.0.0.1', 7563),
    'GammaC': ('127.0.0.1', 7565),
    'DeltaC': ('127.0.0.1', 7567)}

client_addr = ("127.0.0.1", 8080)

# create a client with the above address
client = Client(clientId="my_client_id", ha=client_addr, nodeReg=cliNodeReg)
client.start()
looper.add(client)

idAndKey = client.signer.identifier, client.signer.verkey

for node in alpha, beta, gamma, delta:
    node.clientAuthNr.addClient(*idAndKey)

# client connects
looper.runFor(8)

# Sending the nodes a random msg to nodes
# msg = {"type": "buy", "amount": 100}
msg = {"The_Answer_to_life_is": 42}

# Submit the request to the nodes
request, = client.submit(msg)

# Know the request ID for the submitted request
requestId = request.reqId

# give time for the message to be processed
looper.runFor(20)

# You can get the reply and consensus status of a request by passing the request ID to the getReplyAndStatus
reply, status = client.getReply(requestId)

# print the reply and also, is consensus reached?
print("Reply for the request: {}\n".format(reply))
print("Status: {}\n".format(status))

# Show the reply details of a request
client.showReplyDetails(requestId)

# makeFaulty(alpha, doesntPropagate, altersSignature, subvertsElection)

# As we are using 4 nodes and the consensus can be still achieved with one node being faulty.
makeNodeFaulty(beta, changesRequest)

# Let's send a new message now
msg = {"type": "sell", "amount": 101}

# Submit the msg to the nodes
request2, = client.submit(msg)

# Know the request ID for the submitted request
requestId2 = request2.reqId  # Type: List[ClientRequest]

# give time for the message to be processed
looper.runFor(20)

reply, consensusReached = client.getReply(requestId2)
print("Reply for the request: {}\n".format(reply))
print("Consensus Reached?: {}\n".format(consensusReached))

client.showReplyDetails(requestId2)

looper.runFor(20)

# TODO
"""
[X] We need to clean up creation of a client, the stack should be invisible.
[X] Add some methods to client to make it super simple to expose the messages that came in.
[X] Make it simple to show that consensus was reached on the request.
[ ] Make the one of the nodes malicious, and demonstrate that the protocol still runs.

Also, demonstrate two ways to bootstrap a consensus pool:
1. Pre-shared keys
2. Key-sharing party
"""
