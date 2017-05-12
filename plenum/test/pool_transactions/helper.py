from typing import Iterable, Union

from plenum.client.client import Client
from plenum.client.wallet import Wallet
from plenum.common.constants import STEWARD, TXN_TYPE, NYM, ROLE, TARGET_NYM, ALIAS, \
    NODE_PORT, CLIENT_IP, NODE_IP, DATA, NODE, CLIENT_PORT, VERKEY, SERVICES, \
    VALIDATOR
from plenum.common.keygen_utils import initNodeKeysForBothStacks
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import randomString, hexToFriendly
from plenum.test.helper import waitForSufficientRepliesForRequests
from plenum.test.test_client import TestClient, genTestClient
from plenum.test.test_node import TestNode, check_node_disconnected_from, \
    ensure_node_disconnected, checkNodesConnected
from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa


def sendAddNewClient(role, name, creatorClient, creatorWallet):
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
    return req, wallet


def addNewClient(role, looper, creatorClient: Client, creatorWallet: Wallet,
                 name: str):
    req, wallet = sendAddNewClient(role, name, creatorClient, creatorWallet)
    waitForSufficientRepliesForRequests(looper, creatorClient,
                                        requests=[req], fVal=1)

    return wallet


def sendAddNewNode(newNodeName, stewardClient, stewardWallet,
                   transformOpFunc=None):
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
    if transformOpFunc is not None:
        transformOpFunc(op)

    req = stewardWallet.signOp(op)
    stewardClient.submitReqs(req)
    return req, \
           op[DATA].get(NODE_IP), op[DATA].get(NODE_PORT), \
           op[DATA].get(CLIENT_IP), op[DATA].get(CLIENT_PORT), \
           sigseed


def addNewNode(looper, stewardClient, stewardWallet, newNodeName, tdir, tconf,
               allPluginsPath=None, autoStart=True, nodeClass=TestNode):
    req, nodeIp, nodePort, clientIp, clientPort, sigseed \
        = sendAddNewNode(newNodeName, stewardClient, stewardWallet)
    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req], fVal=1)

    initNodeKeysForBothStacks(newNodeName, tdir, sigseed, override=True)
    node = nodeClass(newNodeName, basedirpath=tdir, config=tconf,
                     ha=(nodeIp, nodePort), cliha=(clientIp, clientPort),
                     pluginPaths=allPluginsPath)
    if autoStart:
        looper.add(node)
    return node


def addNewSteward(looper, tdir,
                  creatorClient, creatorWallet, stewardName,
                  clientClass=TestClient):
    newStewardWallet = addNewClient(STEWARD, looper, creatorClient,
                                    creatorWallet, stewardName)
    newSteward = clientClass(name=stewardName,
                             nodeReg=None, ha=genHa(),
                             basedirpath=tdir)

    looper.add(newSteward)
    looper.run(newSteward.ensureConnectedToNodes())
    return newSteward, newStewardWallet


def addNewStewardAndNode(looper, creatorClient, creatorWallet, stewardName,
                         newNodeName, tdir, tconf, allPluginsPath=None,
                         autoStart=True, nodeClass=TestNode,
                         clientClass=TestClient):

    newSteward, newStewardWallet = addNewSteward(looper, tdir, creatorClient,
                                                 creatorWallet, stewardName,
                                                 clientClass=clientClass)

    newNode = addNewNode(looper, newSteward, newStewardWallet, newNodeName,
                         tdir, tconf, allPluginsPath, autoStart=autoStart,
                         nodeClass=nodeClass)
    return newSteward, newStewardWallet, newNode


def sendChangeNodeHa(stewardClient, stewardWallet, node, nodeHa, clientHa):
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
            ALIAS: node.name,
            SERVICES: [VALIDATOR]
        }
    }

    req = stewardWallet.signOp(op)
    stewardClient.submitReqs(req)
    return req


def changeNodeHa(looper, stewardClient, stewardWallet, node, nodeHa, clientHa):
    req = sendChangeNodeHa(stewardClient, stewardWallet, node, nodeHa, clientHa)
    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req], fVal=1)
    # TODO: Not needed in ZStack, remove once raet is removed
    node.nodestack.clearLocalKeep()
    node.nodestack.clearRemoteKeeps()
    node.clientstack.clearLocalKeep()
    node.clientstack.clearRemoteKeeps()


def changeNodeHaAndReconnect(looper, steward, stewardWallet, node,
                             nodeHa, clientHa,
                             tdirWithPoolTxns, tconf, txnPoolNodeSet):
    changeNodeHa(looper, steward, stewardWallet, node,
                 nodeHa=nodeHa, clientHa=clientHa)
    # restart the Node with new HA
    node.stop()
    looper.removeProdable(name=node.name)
    restartedNode = TestNode(node.name, basedirpath=tdirWithPoolTxns,
                             config=tconf, ha=nodeHa, cliha=clientHa)
    looper.add(restartedNode)

    # replace node in txnPoolNodeSet
    try:
        idx = next(i for i, n in enumerate(txnPoolNodeSet)
                   if n.name == node.name)
    except StopIteration:
        raise Exception('{} is not the pool'.format(node))
    txnPoolNodeSet[idx] = restartedNode

    looper.run(checkNodesConnected(txnPoolNodeSet))
    return restartedNode


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

    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req], fVal=1)

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

    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req], fVal=1)

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
    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req], fVal=1)


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


def disconnect_node_and_ensure_disconnected(looper, poolNodes,
                                            disconnect: Union[str, TestNode],
                                            timeout=None):
    if isinstance(disconnect, TestNode):
        disconnect = disconnect.name
    assert isinstance(disconnect, str)

    disconnectPoolNode(poolNodes, disconnect)
    ensure_node_disconnected(looper, disconnect, poolNodes,
                             timeout=timeout)
