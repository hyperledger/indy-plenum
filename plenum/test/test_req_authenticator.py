import json

import pytest

from indy.did import key_for_did
from plenum.common.constants import TXN_TYPE, DATA, GET_TXN, DOMAIN_LEDGER_ID, NYM
from plenum.common.exceptions import NoAuthenticatorFound
from plenum.common.types import f
from plenum.common.util import randomString
from plenum.server.client_authn import SimpleAuthNr, CoreAuthNr
from plenum.server.req_authenticator import ReqAuthenticator
from plenum.test.helper import sdk_sign_and_submit_op, sdk_send_random_and_check
from plenum.test.pool_transactions.helper import new_client_request
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually

whitelist = ["Given signature is not for current root hash, aborting"]

@pytest.fixture(scope='module')
def pre_reqs():
    simple_authnr = SimpleAuthNr()
    core_authnr = CoreAuthNr([NYM], [GET_TXN], [])
    req_authnr = ReqAuthenticator()
    return simple_authnr, core_authnr, req_authnr


@pytest.fixture(scope='module')
def registration(pre_reqs):
    simple_authnr, core_authnr, req_authnr = pre_reqs
    assert len(req_authnr._authenticators) == 0
    with pytest.raises(RuntimeError):
        req_authnr.core_authenticator

    req_authnr.register_authenticator(core_authnr)
    assert len(req_authnr._authenticators) == 1
    assert req_authnr.core_authenticator == core_authnr

    req_authnr.register_authenticator(simple_authnr)
    assert len(req_authnr._authenticators) == 2


def test_authenticator_registration(pre_reqs, registration):
    simple_authnr, core_authnr, req_authnr = pre_reqs
    assert req_authnr.get_authnr_by_type(CoreAuthNr) == core_authnr


def test_authentication(looper, pre_reqs, registration,
                        sdk_wallet_client,
                        sdk_pool_handle):
    _, core_authnr, req_authnr = pre_reqs

    # Remove simple_authnr
    req_authnr._authenticators = req_authnr._authenticators[:-1]

    # Exception for unknown txn type
    op = {
        TXN_TYPE: 'random_txn_type',
        f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
        DATA: 1
    }
    # Just creating the request
    req = sdk_sign_and_submit_op(looper, sdk_pool_handle,
                                 sdk_wallet_client, op)
    with pytest.raises(NoAuthenticatorFound):
        req_authnr.authenticate(req[0])

    # Empty set for query txn type
    op = {
        TXN_TYPE: GET_TXN,
        f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
        DATA: 1
    }
    # Just creating the request
    req = sdk_sign_and_submit_op(looper, sdk_pool_handle,
                                 sdk_wallet_client, op)
    assert set() == req_authnr.authenticate(req[0])

    # identifier for write type
    wh, did = sdk_wallet_client
    req = new_client_request(None, randomString(), looper, sdk_wallet_client)
    core_authnr.addIdr(did,
                       looper.loop.run_until_complete(key_for_did(sdk_pool_handle, wh, did)))
    assert req_authnr.authenticate(json.loads(req)) == {did, }


def test_propagate_of_ordered_request_doesnt_stash_requests_in_authenticator(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    # Universal delayer
    def stopAll(msg):
        return 100000

    def check_verified_req_list_is_empty():
        for node in txnPoolNodeSet:
            assert len(node.clientAuthNr._verified_reqs) == 0

    # Order one request while cutting off last node
    lastNode = txnPoolNodeSet[-1]
    with delay_rules(lastNode.nodeIbStasher, stopAll), \
         delay_rules(lastNode.clientIbStasher, stopAll):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client, 1)
        old_propagates = [n.spylog.count('processPropagate') for n in txnPoolNodeSet]

    def check_more_propagates_delivered():
        new_propagates = [n.spylog.count('processPropagate') for n in txnPoolNodeSet]
        assert all(old < new for old, new in zip(old_propagates, new_propagates))

    # Wait until more propagates are delivered to all nodes
    looper.run(eventually(check_more_propagates_delivered))

    # Make sure that verified req list will be empty eventually
    looper.run(eventually(check_verified_req_list_is_empty))
