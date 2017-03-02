from typing import Iterable, Union

from plenum.common.keygen_utils import initNodeKeysForBothStacks
from stp_core.network.port_dispenser import genHa

from plenum.client.client import Client
from plenum.client.wallet import Wallet
from stp_core.loop.eventually import eventually
from plenum.common.signer_simple import SimpleSigner
from plenum.common.constants import STEWARD, TXN_TYPE, NYM, ROLE, TARGET_NYM, ALIAS, \
    NODE_PORT, CLIENT_IP, NODE_IP, DATA, NODE, CLIENT_PORT, VERKEY, SERVICES, \
    VALIDATOR
from plenum.common.util import randomString, hexToFriendly
from plenum.test.helper import checkSufficientRepliesReceived
from plenum.test.test_client import TestClient, genTestClient
from plenum.test.test_node import TestNode


def addNewClient(role, looper, creatorClient: Client, creatorWallet: Wallet,
                 name: str):
    wallet = Wallet(name)
    wallet.addIdentifier()
    idr = wallet.defaultId

    op = {
        TXN_TYPE: NYM,
        TARGET_NYM: idr,
        ALIAS: name,
        VERKEY: wallet.getVerkey(idr)
    }

    if role:
        op[ROLE] = role

    req = creatorWallet.signOp(op)
    creatorClient.submitReqs(req)

    nodeCount = len(creatorClient.nodeReg)
    looper.run(eventually(checkSufficientRepliesReceived, creatorClient.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=3 * nodeCount))
    return wallet


def addNewNode(looper, stewardClient, stewardWallet, newNodeName, tdir, tconf,
               allPluginsPath=None, autoStart=True, nodeClass=TestNode):
    sigseed = randomString(32).encode()
    nodeSigner = SimpleSigner(seed=sigseed)

    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)

    op = {
        TXN_TYPE: NODE,
        TARGET_NYM: nodeSigner.identifier,
        DATA: {
            NODE_IP: nodeIp,
            NODE_PORT: nodePort,
            CLIENT_IP: clientIp,
            CLIENT_PORT: clientPort,
            ALIAS: newNodeName,
            SERVICES: [VALIDATOR, ]
        }
    }

    req = stewardWallet.signOp(op)
    stewardClient.submitReqs(req)

    nodeCount = len(stewardClient.nodeReg)
    looper.run(eventually(checkSufficientRepliesReceived, stewardClient.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5 * nodeCount))

    initNodeKeysForBothStacks(newNodeName, tdir, sigseed, override=True)

    node = nodeClass(newNodeName, basedirpath=tdir, config=tconf,
                     ha=(nodeIp, nodePort), cliha=(clientIp, clientPort),
                     pluginPaths=allPluginsPath)
    if autoStart:
        looper.add(node)
    return node


def addNewStewardAndNode(looper, creatorClient, creatorWallet, stewardName,
                         newNodeName, tdir, tconf, allPluginsPath=None,
                         autoStart=True, nodeClass=TestNode,
                         clientClass=TestClient):
    newStewardWallet = addNewClient(STEWARD, looper, creatorClient,
                                    creatorWallet, stewardName)
    newSteward = clientClass(name=stewardName,
                             nodeReg=None, ha=genHa(),
                             basedirpath=tdir)

    looper.add(newSteward)
    looper.run(newSteward.ensureConnectedToNodes())
    newNode = addNewNode(looper, newSteward, newStewardWallet, newNodeName,
                         tdir, tconf, allPluginsPath, autoStart=autoStart,
                         nodeClass=nodeClass)
    return newSteward, newStewardWallet, newNode


def changeNodeHa(looper, stewardClient, stewardWallet, node, nodeHa, clientHa):
    nodeNym = hexToFriendly(node.nodestack.verhex)
    (nodeIp, nodePort), (clientIp, clientPort) = nodeHa, clientHa
    op = {
        TXN_TYPE: NODE,
        TARGET_NYM: nodeNym,
        DATA: {
            NODE_IP: nodeIp,
            NODE_PORT: nodePort,
            CLIENT_IP: clientIp,
            CLIENT_PORT: clientPort,
            ALIAS: node.name
        }
    }

    req = stewardWallet.signOp(op)
    stewardClient.submitReqs(req)
    looper.run(eventually(checkSufficientRepliesReceived, stewardClient.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=2*len(stewardClient.nodeReg)))

    # TODO: Not needed in ZStack, remove once raet is removed
    node.nodestack.clearLocalKeep()
    node.nodestack.clearRemoteKeeps()
    node.clientstack.clearLocalKeep()
    node.clientstack.clearRemoteKeeps()


def changeNodeKeys(looper, stewardClient, stewardWallet, node, verkey):
    nodeNym = hexToFriendly(node.nodestack.verhex)

    op = {
        TXN_TYPE: NODE,
        TARGET_NYM: nodeNym,
        VERKEY: verkey,
        DATA: {
            ALIAS: node.name
        }
    }
    req = stewardWallet.signOp(op)
    stewardClient.submitReqs(req)

    looper.run(eventually(checkSufficientRepliesReceived, stewardClient.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=2*len(stewardClient.nodeReg)))
    node.nodestack.clearLocalRoleKeep()
    node.nodestack.clearRemoteRoleKeeps()
    node.nodestack.clearAllDir()
    node.clientstack.clearLocalRoleKeep()
    node.clientstack.clearRemoteRoleKeeps()
    node.clientstack.clearAllDir()


def suspendNode(looper, stewardClient, stewardWallet, nodeNym, nodeName):
    op = {
        TXN_TYPE: NODE,
        TARGET_NYM: nodeNym,
        DATA: {
            SERVICES: [],
            ALIAS: nodeName
        }
    }
    req = stewardWallet.signOp(op)
    stewardClient.submitReqs(req)
    looper.run(eventually(checkSufficientRepliesReceived, stewardClient.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=2*len(stewardClient.nodeReg)))


def cancelNodeSuspension(looper, stewardClient, stewardWallet, nodeNym,
                         nodeName):
    op = {
        TXN_TYPE: NODE,
        TARGET_NYM: nodeNym,
        DATA: {
            SERVICES: [VALIDATOR],
            ALIAS: nodeName
        }
    }

    req = stewardWallet.signOp(op)
    stewardClient.submitReqs(req)
    looper.run(eventually(checkSufficientRepliesReceived, stewardClient.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=3*len(stewardClient.nodeReg)))


def buildPoolClientAndWallet(clientData, tempDir, clientClass=None,
                             walletClass=None):
    walletClass = walletClass or Wallet
    clientClass = clientClass or TestClient
    name, sigseed = clientData
    w = walletClass(name)
    w.addIdentifier(signer=SimpleSigner(seed=sigseed))
    client, _ = genTestClient(name=name, identifier=w.defaultId,
                              tmpdir=tempDir, usePoolLedger=True,
                              testClientClass=clientClass)
    return client, w


def disconnectPoolNode(poolNodes: Iterable, disconnect: Union[str, TestNode]):
    if isinstance(disconnect, TestNode):
        disconnect = disconnect.name
    assert isinstance(disconnect, str)

    for node in poolNodes:
        if node.name == disconnect:
            node.stop()
        else:
            node.nodestack.disconnectByName(disconnect)


def checkNodeDisconnectedFrom(needle: str, haystack: Iterable[TestNode]):
    """
    Check if the node name given by `needle` is disconnected from nodes in
    `haystack`
    :param needle: Node name which should be disconnected from nodes from
    `haystack`
    :param haystack: nodes who should be disconnected from `needle`
    :return:
    """
    assert all([needle not in node.nodestack.connecteds for node in haystack])


def ensureNodeDisconnectedFromPool(looper, poolNodes,
                                   disconnect: Union[str, TestNode]):
    if isinstance(disconnect, TestNode):
        disconnect = disconnect.name
    assert isinstance(disconnect, str)

    disconnectPoolNode(poolNodes, disconnect)
    looper.run(eventually(checkNodeDisconnectedFrom, disconnect,
                                                    [n for n in poolNodes
                                                     if n.name != disconnect],
                          retryWait=1, timeout=len(poolNodes)-1))
