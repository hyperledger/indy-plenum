from plenum.test.node_catchup.helper import waitNodeDataEquality, ensureClientConnectedToNodesAndPoolLedgerSame
from stp_core.types import HA
from typing import Iterable, Union

from plenum.client.client import Client
from plenum.client.wallet import Wallet
from plenum.common.constants import STEWARD, TXN_TYPE, NYM, ROLE, TARGET_NYM, ALIAS, \
    NODE_PORT, CLIENT_IP, NODE_IP, DATA, NODE, CLIENT_PORT, VERKEY, SERVICES, \
    VALIDATOR, BLS_KEY
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
                                        requests=[req])

    return wallet


def sendAddNewNode(tdir, newNodeName, stewardClient, stewardWallet,
                   transformOpFunc=None):
    sigseed = randomString(32).encode()
    nodeSigner = SimpleSigner(seed=sigseed)
    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)
    _, verkey, bls_key = initNodeKeysForBothStacks(newNodeName, tdir, sigseed, override=True)
    op = {
        TXN_TYPE: NODE,
        TARGET_NYM: nodeSigner.identifier,
        DATA: {
            NODE_IP: nodeIp,
            NODE_PORT: nodePort,
            CLIENT_IP: clientIp,
            CLIENT_PORT: clientPort,
            ALIAS: newNodeName,
            SERVICES: [VALIDATOR, ],
            BLS_KEY: bls_key
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
               allPluginsPath=None, autoStart=True, nodeClass=TestNode,
               transformOpFunc=None):
    nodeClass = nodeClass or TestNode
    req, nodeIp, nodePort, clientIp, clientPort, sigseed \
        = sendAddNewNode(tdir, newNodeName, stewardClient, stewardWallet,
                         transformOpFunc)
    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req])

    # initNodeKeysForBothStacks(newNodeName, tdir, sigseed, override=True)
    # node = nodeClass(newNodeName, basedirpath=tdir, config=tconf,
    #                  ha=(nodeIp, nodePort), cliha=(clientIp, clientPort),
    #                  pluginPaths=allPluginsPath)
    # if autoStart:
    #     looper.add(node)
    # return node
    return start_newly_added_node(looper, newNodeName, tdir, sigseed,
                                  (nodeIp, nodePort), (clientIp, clientPort),
                                  tconf, autoStart, allPluginsPath, nodeClass)


def start_newly_added_node(
        looper,
        node_name,
        tdir,
        sigseed,
        node_ha,
        client_ha,
        conf,
        auto_start,
        plugin_path,
        nodeClass):
    node = nodeClass(node_name, basedirpath=tdir, base_data_dir=tdir, config=conf,
                     ha=node_ha, cliha=client_ha,
                     pluginPaths=plugin_path)
    if auto_start:
        looper.add(node)
    return node


def addNewSteward(looper, tdir,
                  creatorClient, creatorWallet, stewardName,
                  clientClass=TestClient):
    clientClass = clientClass or TestClient
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
                         clientClass=TestClient, transformNodeOpFunc=None):

    newSteward, newStewardWallet = addNewSteward(looper, tdir, creatorClient,
                                                 creatorWallet, stewardName,
                                                 clientClass=clientClass)

    newNode = addNewNode(
        looper,
        newSteward,
        newStewardWallet,
        newNodeName,
        tdir,
        tconf,
        allPluginsPath,
        autoStart=autoStart,
        nodeClass=nodeClass,
        transformOpFunc=transformNodeOpFunc)
    return newSteward, newStewardWallet, newNode


def sendUpdateNode(stewardClient, stewardWallet, node, node_data):
    nodeNym = hexToFriendly(node.nodestack.verhex)
    op = {
        TXN_TYPE: NODE,
        TARGET_NYM: nodeNym,
        DATA: node_data,
    }

    req = stewardWallet.signOp(op)
    stewardClient.submitReqs(req)
    return req


def updateNodeData(looper, stewardClient, stewardWallet, node, node_data):
    req = sendUpdateNode(stewardClient, stewardWallet, node, node_data)
    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req])
    # TODO: Not needed in ZStack, remove once raet is removed
    node.nodestack.clearLocalKeep()
    node.nodestack.clearRemoteKeeps()
    node.clientstack.clearLocalKeep()
    node.clientstack.clearRemoteKeeps()


def updateNodeDataAndReconnect(looper, steward, stewardWallet, node,
                               node_data,
                               tdirWithPoolTxns, tconf, txnPoolNodeSet):
    updateNodeData(looper, steward, stewardWallet, node, node_data)
    # restart the Node with new HA
    node.stop()
    node_alias = node_data.get(ALIAS, None) or node.name
    node_ip = node_data.get(NODE_IP, None) or node.nodestack.ha.host
    node_port = node_data.get(NODE_PORT, None) or node.nodestack.ha.port
    client_ip = node_data.get(CLIENT_IP, None) or node.clientstack.ha.host
    client_port = node_data.get(CLIENT_PORT, None) or node.clientstack.ha.port
    looper.removeProdable(name=node.name)
    restartedNode = TestNode(node_alias, basedirpath=tdirWithPoolTxns, base_data_dir=tdirWithPoolTxns,
                             config=tconf,
                             ha=HA(node_ip, node_port),
                             cliha=HA(client_ip, client_port))
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
                                        requests=[req])

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
                                        requests=[req])


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
                                        requests=[req])


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


def new_client(looper, poolTxnClientData, txnPoolNodeSet, tdirWithPoolTxns):
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              tdirWithPoolTxns)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)
    return client, wallet


def disconnectPoolNode(poolNodes: Iterable,
                       disconnect: Union[str, TestNode], stopNode=True):
    if isinstance(disconnect, TestNode):
        disconnect = disconnect.name
    assert isinstance(disconnect, str)

    for node in poolNodes:
        if node.name == disconnect and stopNode:
            node.stop()
        else:
            node.nodestack.disconnectByName(disconnect)


def reconnectPoolNode(poolNodes: Iterable,
                      connect: Union[str, TestNode], looper):
    if isinstance(connect, TestNode):
        connect = connect.name
    assert isinstance(connect, str)

    for node in poolNodes:
        if node.name == connect:
            node.start(looper)
        else:
            node.nodestack.reconnectRemoteWithName(connect)


def disconnect_node_and_ensure_disconnected(looper, poolNodes,
                                            disconnect: Union[str, TestNode],
                                            timeout=None,
                                            stopNode=True):
    if isinstance(disconnect, TestNode):
        disconnect = disconnect.name
    assert isinstance(disconnect, str)
    disconnectPoolNode(poolNodes, disconnect, stopNode=stopNode)
    ensure_node_disconnected(looper, disconnect, poolNodes,
                             timeout=timeout)


def reconnect_node_and_ensure_connected(looper, poolNodes,
                                        connect: Union[str, TestNode],
                                        timeout=None):
    if isinstance(connect, TestNode):
        connect = connect.name
    assert isinstance(connect, str)

    reconnectPoolNode(poolNodes, connect, looper)
    looper.run(checkNodesConnected(poolNodes, customTimeout=timeout))


def add_2_nodes(looper, existing_nodes, steward, steward_wallet,
                tdir_with_pool_txns, conf, all_plugins_path, names=None):
    assert names is None or (isinstance(names, list) and len(names) == 2)
    names = names or ("Zeta", "Eta")
    new_nodes = []
    for node_name in names:
        new_steward_name = "testClientSteward" + randomString(3)
        new_steward, new_steward_wallet, new_node = addNewStewardAndNode(looper,
                                                                         steward,
                                                                         steward_wallet,
                                                                         new_steward_name,
                                                                         node_name,
                                                                         tdir_with_pool_txns,
                                                                         conf,
                                                                         all_plugins_path)
        existing_nodes.append(new_node)
        looper.run(checkNodesConnected(existing_nodes))
        waitNodeDataEquality(looper, new_node, *existing_nodes[:-1])
        new_nodes.append(new_node)

    return new_nodes
