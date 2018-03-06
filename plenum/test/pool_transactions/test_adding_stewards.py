import pytest

from plenum.common.constants import STEWARD_STRING
from plenum.common.exceptions import RequestRejectedException
from plenum.common.util import randomString
from plenum.test.helper import sdk_get_replies, sdk_eval_timeout, sdk_check_reply
from plenum.test.pool_transactions.helper import sdk_add_new_nym, \
    prepare_nym_request, sdk_sign_and_send_prepared_request


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldThreshold = tconf.stewardThreshold
    tconf.stewardThreshold = 5

    def reset():
        tconf.stewardThreshold = oldThreshold

    request.addfinalizer(reset)
    return tconf


def testOnlyAStewardCanAddAnotherSteward(looper,
                                         txnPoolNodeSet,
                                         sdk_pool_handle,
                                         sdk_wallet_steward,
                                         sdk_wallet_client):
    sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_steward,
                    alias='testSteward' + randomString(3), role=STEWARD_STRING)

    seed = randomString(32)
    wh, _ = sdk_wallet_client

    nym_request, steward_did = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_client, seed,
                            'testSteward2', 'STEWARD'))

    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_client,
                                                        sdk_pool_handle, nym_request)
    total_timeout = sdk_eval_timeout(1, len(txnPoolNodeSet))
    request_couple = sdk_get_replies(looper, [request_couple], total_timeout)[0]
    with pytest.raises(RequestRejectedException) as e:
        sdk_check_reply(request_couple)
    assert 'Only Steward is allowed to do these transactions' in e._excinfo[1].args[0]


def testStewardsCanBeAddedOnlyTillAThresholdIsReached(looper,
                                                      txnPoolNodeSet,
                                                      sdk_pool_handle,
                                                      sdk_wallet_steward,
                                                      tconf):
    sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_steward,
                    alias='testSteward' + randomString(3), role=STEWARD_STRING)
    with pytest.raises(RequestRejectedException) as e:
        sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_steward,
                        alias='testSteward' + randomString(3), role=STEWARD_STRING)
    error_message = 'New stewards cannot be added by other stewards as there ' \
                    'are already {} stewards in the system'.format(tconf.stewardThreshold)
    assert error_message in e._excinfo[1].args[0]
