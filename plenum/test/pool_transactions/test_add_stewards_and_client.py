import json

import base58
import itertools
import pytest

from plenum.common.constants import TARGET_NYM, NODE_IP, CLIENT_IP, NODE_PORT, CLIENT_PORT, DATA
from plenum.common.exceptions import RequestNackedException
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import randomString
from plenum.test import waits
from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.pool_transactions.helper import prepare_new_node_data, prepare_node_request, \
    sdk_sign_and_send_prepared_request
from stp_core.loop.eventually import eventually


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

    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
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
                             sigseed=sigseed,
                             key_proof=key_proof))

    # get hex VerKey
    sigseed = randomString(32).encode()
    nodeSigner = SimpleSigner(seed=sigseed)
    b = base58.b58decode(nodeSigner.identifier)
    hexVerKey = bytearray(b).hex()

    request_json = json.loads(node_request)
    request_json['operation'][TARGET_NYM] = hexVerKey
    node_request = json.dumps(request_json)

    request_couple = sdk_sign_and_send_prepared_request(looper,
                                                        sdk_wallet_new_steward,
                                                        sdk_pool_handle,
                                                        node_request)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [request_couple])
    assert 'client request invalid' in e._excinfo[1].args[0]


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

    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
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
                             sigseed=sigseed,
                             key_proof=key_proof))
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
        request_couple = sdk_sign_and_send_prepared_request(looper,
                                                            sdk_wallet_new_steward,
                                                            sdk_pool_handle,
                                                            node_request1)
        # wait NAcks with exact message. it does not works for just 'is invalid'
        # because the 'is invalid' will check only first few cases
        with pytest.raises(RequestNackedException) as e:
            sdk_get_and_check_replies(looper, [request_couple])
        assert 'invalid network ip address' in e._excinfo[1].args[0] or \
               'expected types' in e._excinfo[1].args[0] or \
               'network port out of the range' in e._excinfo[1].args[0]