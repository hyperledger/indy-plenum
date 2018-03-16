import json

from indy.did import create_and_store_my_did
from indy.ledger import build_node_request, build_nym_request, build_get_txn_request
from indy.pool import refresh_pool_ledger
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from stp_core.loop.looper import Looper
from stp_core.types import HA
from typing import Iterable, Union, Callable

from plenum.client.client import Client
from plenum.client.wallet import Wallet
from plenum.common.constants import STEWARD, TXN_TYPE, NYM, ROLE, TARGET_NYM, ALIAS, \
    NODE_PORT, CLIENT_IP, NODE_IP, DATA, NODE, CLIENT_PORT, VERKEY, SERVICES, \
    VALIDATOR, BLS_KEY, CLIENT_STACK_SUFFIX, STEWARD_STRING
from plenum.common.keygen_utils import initNodeKeysForBothStacks
from plenum.common.signer_simple import SimpleSigner
from plenum.common.signer_did import DidSigner
from plenum.common.util import randomString, hexToFriendly
from plenum.test.helper import waitForSufficientRepliesForRequests, \
    sdk_sign_request_objects, sdk_send_signed_requests, \
    sdk_json_to_request_object, sdk_get_and_check_replies
from plenum.test.test_client import TestClient, genTestClient
from plenum.test.test_node import TestNode, \
    ensure_node_disconnected, checkNodesConnected
from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from plenum.common.config_helper import PNodeConfigHelper


def new_client_request(role, name, creatorWallet):
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

    return creatorWallet.signOp(op), wallet


def sendAddNewClient(role, name, creatorClient, creatorWallet):
    req, wallet = new_client_request(role, name, creatorWallet)
    creatorClient.submitReqs(req)
    return req, wallet


def addNewClient(role, looper, creatorClient: Client, creatorWallet: Wallet,
                 name: str):
    req, wallet = sendAddNewClient(role, name, creatorClient, creatorWallet)
    waitForSufficientRepliesForRequests(looper, creatorClient,
                                        requests=[req])

    return wallet


def sendAddNewNode(tdir, tconf, newNodeName, stewardClient, stewardWallet,
                   transformOpFunc=None):
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort = \
        prepare_new_node_data(tconf, tdir, newNodeName)
    return send_new_node_txn(sigseed,
                             nodeIp, nodePort, clientIp, clientPort,
                             bls_key,
                             newNodeName, stewardClient, stewardWallet,
                             transformOpFunc)


def prepare_new_node_data(tconf, tdir,
                          newNodeName):
    sigseed = randomString(32).encode()
    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)
    config_helper = PNodeConfigHelper(newNodeName, tconf, chroot=tdir)
    _, verkey, bls_key = initNodeKeysForBothStacks(newNodeName, config_helper.keys_dir,
                                                   sigseed, override=True)
    return sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort


def send_new_node_txn(sigseed,
                      nodeIp, nodePort, clientIp, clientPort,
                      bls_key,
                      newNodeName, stewardClient, stewardWallet,
                      transformOpFunc=None):
    nodeSigner = SimpleSigner(seed=sigseed)
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
               transformOpFunc=None, do_post_node_creation: Callable = None):
    nodeClass = nodeClass or TestNode
    req, nodeIp, nodePort, clientIp, clientPort, sigseed \
        = sendAddNewNode(tdir, tconf, newNodeName, stewardClient, stewardWallet,
                         transformOpFunc)
    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req])

    return create_and_start_new_node(looper, newNodeName, tdir, sigseed,
                                     (nodeIp, nodePort), (clientIp, clientPort),
                                     tconf, autoStart, allPluginsPath,
                                     nodeClass,
                                     do_post_node_creation=do_post_node_creation)


def start_not_added_node(looper,
                         tdir, tconf, allPluginsPath,
                         newNodeName):
    '''
    Creates and starts a new node, but doesn't add it to the Pool
    (so, NODE txn is not sent).
    '''
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort = \
        prepare_new_node_data(tconf, tdir, newNodeName)

    new_node = create_and_start_new_node(looper, newNodeName,
                                         tdir, randomString(32).encode(),
                                         (nodeIp, nodePort), (clientIp, clientPort),
                                         tconf, True, allPluginsPath, TestNode)
    return sigseed, bls_key, new_node, (nodeIp, nodePort), (clientIp, clientPort)


def add_started_node(looper,
                     new_node,
                     node_ha,
                     client_ha,
                     txnPoolNodeSet,
                     client_tdir,
                     stewardClient, stewardWallet,
                     sigseed,
                     bls_key):
    '''
    Adds already created node to the pool,
    that is sends NODE txn.
    Makes sure that node is actually added and connected to all otehr nodes.
    '''
    newSteward, newStewardWallet = addNewSteward(looper, client_tdir,
                                                 stewardClient, stewardWallet,
                                                 "Steward" + new_node.name,
                                                 clientClass=TestClient)
    node_name = new_node.name
    send_new_node_txn(sigseed,
                      node_ha[0],
                      node_ha[1],
                      client_ha[0],
                      client_ha[1],
                      bls_key,
                      node_name,
                      newSteward, newStewardWallet)

    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward, *txnPoolNodeSet)

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])


def create_and_start_new_node(
        looper,
        node_name,
        tdir,
        sigseed,
        node_ha,
        client_ha,
        tconf,
        auto_start,
        plugin_path,
        nodeClass,
        do_post_node_creation: Callable = None):
    node = new_node(node_name=node_name,
                    tdir=tdir,
                    node_ha=node_ha,
                    client_ha=client_ha,
                    tconf=tconf,
                    plugin_path=plugin_path,
                    nodeClass=nodeClass)
    if do_post_node_creation:
        do_post_node_creation(node)
    if auto_start:
        looper.add(node)
    return node


def new_node(
        node_name,
        tdir,
        node_ha,
        client_ha,
        tconf,
        plugin_path,
        nodeClass):
    config_helper = PNodeConfigHelper(node_name, tconf, chroot=tdir)
    node = nodeClass(node_name,
                     config_helper=config_helper,
                     config=tconf,
                     ha=node_ha, cliha=client_ha,
                     pluginPaths=plugin_path)
    return node


def addNewSteward(looper, client_tdir,
                  creatorClient, creatorWallet, stewardName,
                  clientClass=TestClient):
    clientClass = clientClass or TestClient
    newStewardWallet = addNewClient(STEWARD, looper, creatorClient,
                                    creatorWallet, stewardName)
    newSteward = clientClass(name=stewardName,
                             nodeReg=None, ha=genHa(),
                             basedirpath=client_tdir)

    looper.add(newSteward)
    looper.run(newSteward.ensureConnectedToNodes())
    return newSteward, newStewardWallet


def addNewStewardAndNode(looper, creatorClient, creatorWallet, stewardName,
                         newNodeName, tdir, client_tdir, tconf, allPluginsPath=None,
                         autoStart=True, nodeClass=TestNode,
                         clientClass=TestClient, transformNodeOpFunc=None,
                         do_post_node_creation: Callable = None):
    newSteward, newStewardWallet = addNewSteward(looper, client_tdir, creatorClient,
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
        transformOpFunc=transformNodeOpFunc,
        do_post_node_creation=do_post_node_creation)
    return newSteward, newStewardWallet, newNode


def sdk_add_new_steward_and_node(looper,
                                 sdk_pool_handle,
                                 sdk_wallet_steward,
                                 new_steward_name,
                                 new_node_name,
                                 tdir,
                                 tconf,
                                 allPluginsPath=None,
                                 autoStart=True,
                                 nodeClass=TestNode,
                                 transformNodeOpFunc=None,
                                 do_post_node_creation: Callable = None,
                                 services=[VALIDATOR]):
    new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                sdk_pool_handle,
                                                sdk_wallet_steward,
                                                alias=new_steward_name,
                                                role=STEWARD_STRING)
    newNode = sdk_add_new_node(
        looper,
        sdk_pool_handle,
        new_steward_wallet_handle,
        new_node_name,
        tdir,
        tconf,
        allPluginsPath,
        autoStart=autoStart,
        nodeClass=nodeClass,
        transformOpFunc=transformNodeOpFunc,
        do_post_node_creation=do_post_node_creation,
        services=services)
    return new_steward_wallet_handle, newNode


def sdk_add_new_nym(looper, sdk_pool_handle, creators_wallet,
                    alias=None, role=None, seed=None):
    seed = seed or randomString(32)
    wh, _ = creators_wallet

    # filling nym request and getting steward did
    # if role == None, we are adding client
    nym_request, new_did = looper.loop.run_until_complete(
        prepare_nym_request(creators_wallet, seed,
                            alias, role))

    # sending request using 'sdk_' functions
    request_couple = sdk_sign_and_send_prepared_request(looper, creators_wallet,
                                                        sdk_pool_handle, nym_request)

    # waitng for replies
    sdk_get_and_check_replies(looper, [request_couple])
    return wh, new_did


def sdk_add_new_node(looper,
                     sdk_pool_handle,
                     steward_wallet_handle,
                     new_node_name,
                     tdir, tconf,
                     allPluginsPath=None, autoStart=True, nodeClass=TestNode,
                     transformOpFunc=None, do_post_node_creation: Callable = None,
                     services=[VALIDATOR]):
    nodeClass = nodeClass or TestNode
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort = \
        prepare_new_node_data(tconf, tdir, new_node_name)

    # filling node request
    _, steward_did = steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=services))

    # sending request using 'sdk_' functions
    request_couple = sdk_sign_and_send_prepared_request(looper, steward_wallet_handle,
                                                        sdk_pool_handle, node_request)

    # waitng for replies
    sdk_get_and_check_replies(looper, [request_couple])

    return create_and_start_new_node(looper, new_node_name, tdir, sigseed,
                                     (nodeIp, nodePort), (clientIp, clientPort),
                                     tconf, autoStart, allPluginsPath,
                                     nodeClass,
                                     do_post_node_creation=do_post_node_creation)


async def prepare_nym_request(wallet, named_seed, alias, role):
    wh, submitter_did = wallet
    (named_did, named_verkey) = await create_and_store_my_did(wh,
                                                              json.dumps({
                                                                  'seed': named_seed,
                                                                  'cid': True})
                                                              )
    nym_request = await build_nym_request(submitter_did, named_did, named_verkey,
                                          alias, role)
    return nym_request, named_did


async def prepare_node_request(steward_did, new_node_name=None, clientIp=None,
                               clientPort=None, nodeIp=None, nodePort=None, bls_key=None,
                               sigseed=None, destination=None, services=[VALIDATOR]):
    use_sigseed = sigseed is not None
    use_dest = destination is not None
    if use_sigseed == use_dest:
        raise AttributeError('You should provide only one of: sigseed or destination')
    if use_sigseed:
        nodeSigner = SimpleSigner(seed=sigseed)
        destination = nodeSigner.identifier

    data = {}
    if new_node_name is not None:
        data['alias'] = new_node_name
    if clientIp is not None:
        data['client_ip'] = clientIp
    if clientPort is not None:
        data['client_port'] = clientPort
    if nodeIp is not None:
        data['node_ip'] = nodeIp
    if nodePort is not None:
        data['node_port'] = nodePort
    if bls_key is not None:
        data['blskey'] = bls_key
    if services is not None:
        data['services'] = services

    node_request = await build_node_request(steward_did, destination, json.dumps(data))
    return node_request


def sdk_sign_and_send_prepared_request(looper, sdk_wallet, sdk_pool_handle, string_req):
    signed_reqs = sdk_sign_request_objects(looper, sdk_wallet,
                                           [sdk_json_to_request_object(
                                               json.loads(string_req))])
    request_couple = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)[0]
    return request_couple


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


def sdk_send_update_node(looper, sdk_submitter_wallet, sdk_pool_handle,
                         destination, alias,
                         node_ip, node_port,
                         client_ip, client_port,
                         services=[VALIDATOR]):
    _, submitter_did = sdk_submitter_wallet
    # filling node request
    node_request = looper.loop.run_until_complete(
        prepare_node_request(submitter_did,
                             new_node_name=alias,
                             clientIp=client_ip,
                             clientPort=client_port,
                             nodeIp=node_ip,
                             nodePort=node_port,
                             destination=destination,
                             services=services))

    # sending request using 'sdk_' functions
    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_submitter_wallet,
                                                        sdk_pool_handle, node_request)

    # waitng for replies
    reply = sdk_get_and_check_replies(looper, [request_couple])[0][1]
    return reply


def updateNodeData(looper, stewardClient, stewardWallet, node, node_data):
    req = sendUpdateNode(stewardClient, stewardWallet, node, node_data)
    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req])


def sdk_pool_refresh(looper, sdk_pool_handle):
    looper.loop.run_until_complete(
        refresh_pool_ledger(sdk_pool_handle))


def sdk_build_get_txn_request(looper, steward_did, data):
    request = looper.loop.run_until_complete(
        build_get_txn_request(steward_did, data))
    return request


def update_node_data_and_reconnect(looper, txnPoolNodeSet,
                                   steward_wallet,
                                   sdk_pool_handle,
                                   node,
                                   new_node_ip, new_node_port,
                                   new_client_ip, new_client_port,
                                   tdir, tconf):
    node_ha = node.nodestack.ha
    cli_ha = node.clientstack.ha
    node_dest = hexToFriendly(node.nodestack.verhex)
    sdk_send_update_node(looper, steward_wallet, sdk_pool_handle,
                         node_dest, node.name,
                         new_node_ip, new_node_port,
                         new_client_ip, new_client_port)
    # restart the Node with new HA
    node.stop()
    looper.removeProdable(name=node.name)
    config_helper = PNodeConfigHelper(node.name, tconf, chroot=tdir)
    restartedNode = TestNode(node.name,
                             config_helper=config_helper,
                             config=tconf,
                             ha=HA(new_node_ip or node_ha.host,
                                   new_node_port or node_ha.port),
                             cliha=HA(new_client_ip or cli_ha.host,
                                      new_client_port or cli_ha.port))
    looper.add(restartedNode)

    # replace node in txnPoolNodeSet
    try:
        idx = next(i for i, n in enumerate(txnPoolNodeSet)
                   if n.name == node.name)
    except StopIteration:
        raise Exception('{} is not the pool'.format(node))
    txnPoolNodeSet[idx] = restartedNode

    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)
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


def sdk_change_node_keys(looper, node, sdk_wallet_steward, sdk_pool_handle, verkey):
    _, steward_did = sdk_wallet_steward
    node_dest = hexToFriendly(node.nodestack.verhex)
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=node.name,
                             destination=node_dest))

    request_json = json.loads(node_request)
    request_json['operation'][VERKEY] = verkey
    node_request1 = json.dumps(request_json)

    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_steward,
                                                        sdk_pool_handle, node_request1)
    sdk_get_and_check_replies(looper, [request_couple])

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


def buildPoolClientAndWallet(clientData, tempDir, clientClass=None, walletClass=None):
    walletClass = walletClass or Wallet
    clientClass = clientClass or TestClient
    name, sigseed = clientData
    w = walletClass(name)
    w.addIdentifier(signer=DidSigner(seed=sigseed))
    client, _ = genTestClient(name=name, identifier=w.defaultId,
                              tmpdir=tempDir, usePoolLedger=True,
                              testClientClass=clientClass)
    return client, w


def new_client(looper, poolTxnClientData, txnPoolNodeSet, client_tdir):
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              client_tdir)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)
    return client, wallet


def disconnectPoolNode(poolNodes: Iterable,
                       disconnect: Union[str, TestNode],
                       stopNode=True):
    if isinstance(disconnect, TestNode):
        disconnect = disconnect.name
    assert isinstance(disconnect, str)

    for node in poolNodes:
        if node.name == disconnect:
            if stopNode:
                node.stop()
            else:
                node.clientstack.close()
                node.nodestack.close()
            break
    else:
        raise AssertionError('The node {} which should be disconnected '
                             'is not found in the passed pool node list {}'
                             .format(disconnect, poolNodes))

    for node in poolNodes:
        if node.name != disconnect:
            node.nodestack.disconnectByName(disconnect)


def reconnectPoolNode(looper: Looper,
                      poolNodes: Iterable,
                      connect: Union[str, TestNode]):
    if isinstance(connect, TestNode):
        connect = connect.name
    assert isinstance(connect, str)

    for node in poolNodes:
        if node.name == connect:
            if node.isGoing():
                node.nodestack.open()
                node.clientstack.open()
                node.nodestack.maintainConnections(force=True)
            else:
                node.start(looper)
            break
    else:
        raise AssertionError('The node {} which should be reconnected '
                             'is not found in the passed pool node list {}'
                             .format(connect, poolNodes))

    for node in poolNodes:
        if node.name != connect:
            node.nodestack.reconnectRemoteWithName(connect)


def disconnect_node_and_ensure_disconnected(looper: Looper,
                                            poolNodes: Iterable[TestNode],
                                            disconnect: Union[str, TestNode],
                                            timeout=None,
                                            stopNode=True):
    if isinstance(disconnect, TestNode):
        disconnect = disconnect.name
    assert isinstance(disconnect, str)

    matches = [n for n in poolNodes if n.name == disconnect]
    assert len(matches) == 1
    node_to_disconnect = matches[0]

    disconnectPoolNode(poolNodes, disconnect, stopNode=stopNode)
    ensure_node_disconnected(looper,
                             node_to_disconnect,
                             set(poolNodes) - {node_to_disconnect},
                             timeout=timeout)


def reconnect_node_and_ensure_connected(looper: Looper,
                                        poolNodes: Iterable[TestNode],
                                        connect: Union[str, TestNode],
                                        timeout=None):
    if isinstance(connect, TestNode):
        connect = connect.name
    assert isinstance(connect, str)

    reconnectPoolNode(looper, poolNodes, connect)
    looper.run(checkNodesConnected(poolNodes, customTimeout=timeout))


def add_2_nodes(looper, existing_nodes, steward, steward_wallet,
                tdir, client_tdir, tconf, all_plugins_path, names=None):
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
                                                                         tdir,
                                                                         client_tdir,
                                                                         tconf,
                                                                         all_plugins_path)
        existing_nodes.append(new_node)
        looper.run(checkNodesConnected(existing_nodes))
        waitNodeDataEquality(looper, new_node, *existing_nodes[:-1])
        new_nodes.append(new_node)

    return new_nodes


def sdk_add_2_nodes(looper, txnPoolNodeSet,
                    sdk_pool_handle, sdk_wallet_steward,
                    tdir, tconf, allPluginsPath):
    names = ("Zeta", "Eta")
    new_nodes = []
    for node_name in names:
        new_steward_name = "testClientSteward" + randomString(3)
        new_steward_wallet, new_node = \
            sdk_add_new_steward_and_node(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_steward,
                                         new_steward_name,
                                         node_name,
                                         tdir,
                                         tconf,
                                         allPluginsPath)
        txnPoolNodeSet.append(new_node)
        looper.run(checkNodesConnected(txnPoolNodeSet))
        waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
        sdk_pool_refresh(looper, sdk_pool_handle)
        new_nodes.append(new_node)
    return new_nodes
