import base64

from raet.nacling import Privateer

from plenum.client.signer import SimpleSigner
from plenum.common.raet import initLocalKeep
from plenum.common.txn import TXN_TYPE, ORIGIN, TARGET_NYM, DATA, PUBKEY, ALIAS, \
    NEW_NODE, NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, CHANGE_HA, NEW_STEWARD, \
    CHANGE_KEYS, VERKEY
from plenum.common.types import HA
from plenum.common.util import randomString
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd, genHa, TestNode, \
    TestClient


def addNewClient(typ, looper, client, name):
    sigseed = randomString(32).encode()
    pkseed = randomString(32).encode()
    newSigner = SimpleSigner(seed=sigseed)
    priver = Privateer(pkseed)
    req = client.submitNewClient(typ, name, priver.pubhex.decode(),
                                 newSigner.verkey.decode())
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=7))
    return newSigner


def addNewNode(looper, client, newNodeName, tdir, tconf, statsConsumersPluginPath):
    sigseed = randomString(32).encode()
    pkseed = randomString(32).encode()
    newSigner = SimpleSigner(seed=sigseed)
    priver = Privateer(pkseed)
    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)
    req = client.submitNewNode(newNodeName, priver.pubhex.decode(),
                                 newSigner.verkey.decode(),
                               HA(nodeIp, nodePort), HA(clientIp, clientPort))
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    initLocalKeep(newNodeName, tdir, pkseed, sigseed,
                  override=True)
    node = TestNode(newNodeName, basedirpath=tdir, config=tconf,
                    ha=(nodeIp, nodePort), cliha=(clientIp, clientPort),
                    statsConsumersPluginPath=statsConsumersPluginPath)
    looper.add(node)
    return node


def addNewStewardAndNode(looper, client, stewardName, newNodeName, nodeReg,
                         tdir, tconf):
    newStewardSigner = addNewClient(NEW_STEWARD, looper, client, stewardName)
    newSteward = TestClient(name=stewardName,
                            nodeReg=nodeReg, ha=genHa(),
                            signer=newStewardSigner,
                            basedirpath=tdir)

    looper.add(newSteward)
    looper.run(newSteward.ensureConnectedToNodes())
    looper.runFor(.25)
    newNode = addNewNode(looper, newSteward, newNodeName, tdir, tconf)
    return newSteward, newNode


def changeNodeIp(looper, client, node, nodeHa, clientHa, baseDir, conf):
    nodeNym = base64.b64encode(node.nodestack.local.signer.verraw).decode()
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


def changeNodeKeys(looper, client, node, verkey, pubkey, baseDir, conf):
    nodeNym = base64.b64encode(node.nodestack.local.signer.verraw).decode()
    req = client.submitNodeKeysChange(node.name, nodeNym, verkey, pubkey)
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    node.nodestack.clearLocalRoleKeep()
    node.nodestack.clearRemoteRoleKeeps()
    node.nodestack.clearAllDir()
    node.clientstack.clearLocalRoleKeep()
    node.clientstack.clearRemoteRoleKeeps()
    node.clientstack.clearAllDir()
