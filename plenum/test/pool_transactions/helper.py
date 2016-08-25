from plenum.common.txn import STEWARD
from raet.nacling import Privateer

from plenum.client.signer import SimpleSigner
from plenum.common.raet import initLocalKeep
from plenum.common.types import HA
from plenum.common.util import randomString, hexToCryptonym
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd, genHa, TestNode, \
    TestClient


def addNewClient(role, looper, client, name):
    sigseed = randomString(32).encode()
    newSigner = SimpleSigner(seed=sigseed)
    req = client.submitNewClient(role, name, newSigner.verkey.decode())
    nodeCount = len(client.nodeReg)
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=3*nodeCount))
    return newSigner


def addNewNode(looper, client, newNodeName, tdir, tconf, allPluginsPath=None,
               autoStart=True):
    sigseed = randomString(32).encode()
    newSigner = SimpleSigner(seed=sigseed)
    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)
    req = client.submitNewNode(newNodeName, newSigner.verkey.decode(),
                               HA(nodeIp, nodePort), HA(clientIp, clientPort))
    nodeCount = len(client.nodeReg)
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=3*nodeCount))
    initLocalKeep(newNodeName, tdir, sigseed, override=True)
    node = TestNode(newNodeName, basedirpath=tdir, config=tconf,
                    ha=(nodeIp, nodePort), cliha=(clientIp, clientPort),
                    pluginPaths=allPluginsPath)
    if autoStart:
        looper.add(node)
    return node


def addNewStewardAndNode(looper, client, stewardName, newNodeName, tdir, tconf,
                         allPluginsPath=None, autoStart=True):
    newStewardSigner = addNewClient(STEWARD, looper, client, stewardName)
    newSteward = TestClient(name=stewardName,
                            nodeReg=None, ha=genHa(),
                            signer=newStewardSigner,
                            basedirpath=tdir)

    looper.add(newSteward)
    looper.run(newSteward.ensureConnectedToNodes())
    newNode = addNewNode(looper, newSteward, newNodeName, tdir, tconf,
                         allPluginsPath, autoStart=autoStart)
    return newSteward, newNode


def changeNodeIp(looper, client, node, nodeHa, clientHa):
    nodeNym = hexToCryptonym(node.nodestack.local.signer.verhex)
    (nodeIp, nodePort), (clientIp, clientPort) = nodeHa, clientHa
    req = client.submitNodeIpChange(node.name, nodeNym, HA(nodeIp, nodePort),
                                    HA(clientIp, clientPort))
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    node.nodestack.clearLocalKeep()
    node.nodestack.clearRemoteKeeps()
    node.clientstack.clearLocalKeep()
    node.clientstack.clearRemoteKeeps()


def changeNodeKeys(looper, client, node, verkey):
    nodeNym = hexToCryptonym(node.nodestack.local.signer.verhex)
    req = client.submitNodeKeysChange(node.name, nodeNym, verkey)
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    node.nodestack.clearLocalRoleKeep()
    node.nodestack.clearRemoteRoleKeeps()
    node.nodestack.clearAllDir()
    node.clientstack.clearLocalRoleKeep()
    node.clientstack.clearRemoteRoleKeeps()
    node.clientstack.clearAllDir()
