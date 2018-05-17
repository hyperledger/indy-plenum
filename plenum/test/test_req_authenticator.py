import json

import pytest

from indy.did import key_for_did
from plenum.common.constants import TXN_TYPE, DATA, GET_TXN, DOMAIN_LEDGER_ID
from plenum.common.exceptions import NoAuthenticatorFound
from plenum.common.types import f
from plenum.common.util import randomString
from plenum.server.client_authn import SimpleAuthNr, CoreAuthNr
from plenum.server.req_authenticator import ReqAuthenticator
from plenum.test.helper import sdk_sign_and_submit_op
from plenum.test.pool_transactions.helper import new_client_request


@pytest.fixture(scope='module')
def pre_reqs():
    simple_authnr = SimpleAuthNr()
    core_authnr = CoreAuthNr()
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
