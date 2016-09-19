from plenum.client.client import Client
from plenum.client.wallet import Wallet
from plenum.common.txn import STEWARD, TXN_TYPE, NYM, ROLE, TARGET_NYM, ALIAS, \
    NODE_PORT, CLIENT_IP, NODE_IP, DATA, NEW_NODE, CLIENT_PORT

from plenum.client.signer import SimpleSigner
from plenum.common.raet import initLocalKeep
from plenum.common.types import HA
from plenum.common.util import randomString, hexToCryptonym
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd, genHa, TestNode, \
    TestClient


def addNewClient(role, looper, client: Client, name):
    wallet = Wallet(name)
    wallet.addSigner()
    verstr = wallet._getIdData().signer.verstr

    op = {
        TXN_TYPE: NYM,
        ROLE: role,
        TARGET_NYM: verstr,
        ALIAS: name
    }

    req = wallet.signOp(op)
    client.submitReqs(req)

    # DEPR
    # req = client.submitNewClient(role, name, newSigner.verkey.decode())
    nodeCount = len(client.nodeReg)
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=3*nodeCount))
    return wallet._getIdData().signer


def addNewNode(looper, client, newNodeName, tdir, tconf, allPluginsPath=None,
               autoStart=True):
    wallet = Wallet(client.name)
    sigseed = randomString(32).encode()
    newSigner = SimpleSigner(seed=sigseed)

    wallet.addSigner(newSigner)
    verstr = wallet._getIdData().signer.verstr
    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)

    op = {
        TXN_TYPE: NEW_NODE,
        TARGET_NYM: verstr,
        DATA: {
            NODE_IP: nodeIp,
            NODE_PORT: nodePort,
            CLIENT_IP: clientIp,
            CLIENT_PORT: clientPort,
            ALIAS: newNodeName
        }
    }

    req = wallet.signOp(op)
    client.submitReqs(req)

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
                            # DEPR
                            # signer=newStewardSigner,
                            basedirpath=tdir)

    looper.add(newSteward)
    looper.run(newSteward.ensureConnectedToNodes())
    newNode = addNewNode(looper, newSteward, newNodeName, tdir, tconf,
                         allPluginsPath, autoStart=autoStart)
    return newSteward, newNode


def changeNodeHa(looper, client, node, nodeHa, clientHa):
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
