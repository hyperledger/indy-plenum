import pytest

from plenum.common.constants import STEWARD_STRING
from plenum.common.exceptions import RequestRejectedException
from plenum.common.util import randomString
from plenum.test.helper import vdr_get_replies, vdr_eval_timeout, vdr_check_reply
from plenum.test.pool_transactions.helper import vdr_add_new_nym, \
    vdr_prepare_nym_request, vdr_sign_and_send_prepared_request


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldThreshold = tconf.stewardThreshold
    tconf.stewardThreshold = 6

    def reset():
        tconf.stewardThreshold = oldThreshold

    request.addfinalizer(reset)
    return tconf


def testOnlyAStewardCanAddAnotherSteward(looper,
                                         txnPoolNodeSet,
                                         vdr_pool_handle,
                                         vdr_wallet_steward,
                                         vdr_wallet_client):
    vdr_add_new_nym(looper, vdr_pool_handle, vdr_wallet_steward,
                    alias='testSteward' + randomString(3), role=STEWARD_STRING)

    seed = randomString(32)
    wh, _ = vdr_wallet_client

    nym_request, steward_did = looper.loop.run_until_complete(
        vdr_prepare_nym_request(vdr_wallet_client, seed,
                            'testSteward2', 'STEWARD'))

    request_couple = vdr_sign_and_send_prepared_request(looper, vdr_wallet_client,
                                                        vdr_pool_handle, nym_request)
    total_timeout = vdr_eval_timeout(1, len(txnPoolNodeSet))
    request_couple = vdr_get_replies(looper, [request_couple], total_timeout)[0]
    with pytest.raises(RequestRejectedException) as e:
        vdr_check_reply(request_couple)
    assert 'Only Steward is allowed to do these transactions' in e._excinfo[1].args[0]


def testStewardsCanBeAddedOnlyTillAThresholdIsReached(looper,
                                                      txnPoolNodeSet,
                                                      vdr_pool_handle,
                                                      vdr_wallet_steward,
                                                      tconf):
    vdr_add_new_nym(looper, vdr_pool_handle, vdr_wallet_steward,
                    alias='testSteward' + randomString(3), role=STEWARD_STRING)
    with pytest.raises(RequestRejectedException) as e:
        vdr_add_new_nym(looper, vdr_pool_handle, vdr_wallet_steward,
                        alias='testSteward' + randomString(3), role=STEWARD_STRING)
    error_message = 'New stewards cannot be added by other stewards as there ' \
                    'are already {} stewards in the system'.format(tconf.stewardThreshold)
    assert error_message in e._excinfo[1].args[0]
