import itertools
import json

import base58
import pytest

from plenum.common.exceptions import RequestRejectedException, RequestNackedException
from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.common.constants import DATA, TARGET_NYM, \
    NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, STEWARD_STRING
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import getMaxFailures, randomString
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check, sdk_get_and_check_replies
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

def testStewardCannotAddMoreThanOneNode(looper, txnPoolNodeSet, sdk_pool_handle,
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


def testNonStewardCannotAddNode(looper, txnPoolNodeSet, sdk_pool_handle,
                                sdk_wallet_client, tdir, tconf,
                                allPluginsPath):
    new_node_name = "Epsilon"
    with pytest.raises(RequestRejectedException) as e:
        sdk_add_new_node(looper,
                         sdk_pool_handle,
                         sdk_wallet_client,
                         new_node_name,
                         tdir,
                         tconf,
                         allPluginsPath)
    assert 'is not a steward so cannot add a ' in e._excinfo[1].args[0]
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
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)


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
                                                alias='New steward' + randomString(3),
                                                role=STEWARD_STRING)
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort = \
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
                             sigseed=sigseed))

    # case from the ticket
    request_json = json.loads(node_request)
    request_json['operation'][DATA][NODE_PORT + ' '] = request_json['operation'][DATA][NODE_PORT]
    del request_json['operation'][DATA][NODE_PORT]
    node_request1 = json.dumps(request_json)

    request_couple = sdk_sign_and_send_prepared_request(looper, new_steward_wallet_handle,
                                                        sdk_pool_handle, node_request1)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [request_couple])
    assert 'unknown field' in e._excinfo[1].args[0]

    for fn in (NODE_IP, CLIENT_IP, NODE_PORT, CLIENT_PORT):
        request_json = json.loads(node_request)
        del request_json['operation'][DATA][fn]
        node_request2 = json.dumps(request_json)
        request_couple = sdk_sign_and_send_prepared_request(looper, new_steward_wallet_handle,
                                                            sdk_pool_handle, node_request2)
        # wait NAcks with exact message. it does not works for just 'is missed'
        # because the 'is missed' will check only first few cases
        with pytest.raises(RequestNackedException) as e:
            sdk_get_and_check_replies(looper, [request_couple])
        assert 'missed fields' in e._excinfo[1].args[0]


def testNodesConnect(txnPoolNodeSet):
    pass


def testNodesReceiveClientMsgs(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def testAddNewClient(looper, txnPoolNodeSet, sdk_wallet_new_client):
    _, did = sdk_wallet_new_client

    def chk():
        for node in txnPoolNodeSet:
            assert did in \
                   node.clientAuthNr.core_authenticator.clients

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))


def testStewardCannotAddNodeWithNonBase58VerKey(looper, tdir, tconf,
                                                txnPoolNodeSet,
                                                sdk_pool_handle,
                                                sdk_wallet_new_steward):
    """
    The Case:
        Steward accidentally sends the NODE txn with a non base58 verkey.
    The expected result:
        Steward gets NAck response from the pool.
    """
    new_node_name = "Epsilon"

    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort = \
        prepare_new_node_data(tconf, tdir, new_node_name)
    _, steward_did = sdk_wallet_new_steward
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed))

    # get hex VerKey
    sigseed = randomString(32).encode()
    nodeSigner = SimpleSigner(seed=sigseed)
    b = base58.b58decode(nodeSigner.identifier)
    hexVerKey = bytearray(b).hex()

    request_json = json.loads(node_request)
    request_json['operation'][TARGET_NYM] = hexVerKey
    node_request = json.dumps(request_json)

    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_new_steward,
                                                        sdk_pool_handle, node_request)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [request_couple])
    assert 'should not contain the following chars' in e._excinfo[1].args[0]


def testStewardCannotAddNodeWithInvalidHa(looper, tdir, tconf,
                                          txnPoolNodeSet,
                                          sdk_wallet_new_steward,
                                          sdk_pool_handle):
    """
    The case:
        Steward accidentally sends the NODE txn with an invalid HA.
    The expected result:
        Steward gets NAck response from the pool.
    """
    new_node_name = "Epsilon"

    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort = \
        prepare_new_node_data(tconf, tdir, new_node_name)
    _, steward_did = sdk_wallet_new_steward
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed))
    # a sequence of the test cases for each field
    tests = itertools.chain(
        itertools.product(
            (NODE_IP, CLIENT_IP), ('127.0.0.1 ', '256.0.0.1', '0.0.0.0')
        ),
        itertools.product(
            (NODE_PORT, CLIENT_PORT), ('foo', '9700',
                                       0, 65535 + 1, 4351683546843518184)
        ),
    )

    for field, value in tests:
        # create a transform function for each test
        request_json = json.loads(node_request)
        request_json['operation'][DATA][field] = value
        node_request1 = json.dumps(request_json)
        request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_new_steward,
                                                            sdk_pool_handle, node_request1)
        # wait NAcks with exact message. it does not works for just 'is invalid'
        # because the 'is invalid' will check only first few cases
        with pytest.raises(RequestNackedException) as e:
            sdk_get_and_check_replies(looper, [request_couple])
        assert 'invalid network ip address' in e._excinfo[1].args[0] or \
               'expected types' in e._excinfo[1].args[0] or \
               'network port out of the range' in e._excinfo[1].args[0]
