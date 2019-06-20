import json

import pytest

from plenum.common.exceptions import RequestRejectedException, \
    RequestNackedException

from plenum.common.constants import DATA, \
    NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, STEWARD_STRING
from plenum.common.util import getMaxFailures, randomString
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_get_and_check_replies
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_add_new_node, \
    sdk_add_2_nodes, sdk_pool_refresh, sdk_add_new_nym, prepare_new_node_data, \
    prepare_node_request, sdk_sign_and_send_prepared_request
from plenum.test.test_node import checkProtocolInstanceSetup
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()


# Whitelisting "got error while verifying message" since a node while not have
# initialised a connection for a new node by the time the new node's message
# reaches it


def testStewardCannotAddMoreThanOneNode(looper, txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_steward, tdir, tconf,
                                        allPluginsPath):
    new_node_name = "Epsilon"
    with pytest.raises(RequestRejectedException) as e:
        sdk_add_new_node(looper,
                         sdk_pool_handle,
                         sdk_wallet_steward,
                         new_node_name,
                         tdir,
                         tconf,
                         allPluginsPath)
    assert 'already has a node' in e._excinfo[1].args[0]
    sdk_pool_refresh(looper, sdk_pool_handle)


def testClientConnectsToNewNode(looper,
                                sdk_pool_handle,
                                txnPoolNodeSet,
                                sdk_node_theta_added,
                                sdk_wallet_client):
    """
    A client should be able to connect to a newly added node
    """
    _, new_node = sdk_node_theta_added
    logger.debug("{} connected to the pool".format(new_node))
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)


def testAdd2NewNodes(looper, txnPoolNodeSet,
                     sdk_pool_handle, sdk_wallet_steward,
                     tdir, tconf, allPluginsPath):
    """
    Add 2 new nodes to trigger replica addition and primary election
    """
    new_nodes = sdk_add_2_nodes(looper, txnPoolNodeSet,
                                sdk_pool_handle, sdk_wallet_steward,
                                tdir, tconf, allPluginsPath)
    for n in new_nodes:
        logger.debug("{} connected to the pool".format(n))

    f = getMaxFailures(len(txnPoolNodeSet))

    def checkFValue():
        for node in txnPoolNodeSet:
            assert node.f == f
            assert len(node.replicas) == (f + 1)

    timeout = waits.expectedClientToPoolConnectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(checkFValue, retryWait=1, timeout=timeout))
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    sdk_pool_refresh(looper, sdk_pool_handle)


def testStewardCannotAddNodeWithOutFullFieldsSet(looper, tdir, tconf,
                                                 txnPoolNodeSet,
                                                 sdk_pool_handle,
                                                 sdk_wallet_steward):
    """
    The case:
        Steward accidentally sends the NODE txn without full fields set.
    The expected result:
        Steward gets NAck response from the pool.
    """
    new_node_name = "Epsilon"

    new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                sdk_pool_handle,
                                                sdk_wallet_steward,
                                                alias='New steward' + randomString(
                                                    3),
                                                role=STEWARD_STRING)
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, new_node_name)
    _, steward_did = new_steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             key_proof=key_proof))

    # case from the ticket
    request_json = json.loads(node_request)
    request_json['operation'][DATA][NODE_PORT + ' '] = \
        request_json['operation'][DATA][NODE_PORT]
    del request_json['operation'][DATA][NODE_PORT]
    node_request1 = json.dumps(request_json)

    request_couple = sdk_sign_and_send_prepared_request(looper,
                                                        new_steward_wallet_handle,
                                                        sdk_pool_handle,
                                                        node_request1)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [request_couple])
    assert 'missed fields - node_port' in e._excinfo[1].args[0]

    for fn in (NODE_IP, CLIENT_IP, NODE_PORT, CLIENT_PORT):
        request_json = json.loads(node_request)
        del request_json['operation'][DATA][fn]
        node_request2 = json.dumps(request_json)
        request_couple = sdk_sign_and_send_prepared_request(looper,
                                                            new_steward_wallet_handle,
                                                            sdk_pool_handle,
                                                            node_request2)
        # wait NAcks with exact message. it does not works for just 'is missed'
        # because the 'is missed' will check only first few cases
        with pytest.raises(RequestNackedException) as e:
            sdk_get_and_check_replies(looper, [request_couple])
        assert 'missed fields' in e._excinfo[1].args[0]


def testNodesConnect(txnPoolNodeSet):
    pass


def testNodesReceiveClientMsgs(looper, txnPoolNodeSet, sdk_wallet_client,
                               sdk_pool_handle):
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client,
                               sdk_pool_handle)
