import pytest

from plenum.common.constants import STEWARD
from plenum.common.util import randomString
from plenum.test.helper import waitRejectWithReason, sdk_get_replies, sdk_eval_timeout, sdk_check_reply
from plenum.test.pool_transactions.helper import addNewClient, sendAddNewClient, sdk_add_new_steward, \
    prepare_nym_request, sdk_sign_and_send_prepared_request


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldThreshold = tconf.stewardThreshold
    tconf.stewardThreshold = 5

    def reset():
        tconf.stewardThreshold = oldThreshold

    request.addfinalizer(reset)
    return tconf


def testOnlyAStewardCanAddAnotherSteward(looper, txnPoolNodeSet,
                                         tdirWithPoolTxns, poolTxnClientData,
                                         sdk_pool_handle, sdk_wallet_steward,
                                         sdk_wallet_client):
    sdk_add_new_steward(looper, sdk_pool_handle, sdk_wallet_steward, 'testSteward1')

    seed = randomString(32)
    wh, _ = sdk_wallet_client

    nym_request, steward_did = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_client, seed,
                            'testSteward2', 'STEWARD'))

    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_client,
                                                        sdk_pool_handle, nym_request)
    total_timeout = sdk_eval_timeout(1, len(txnPoolNodeSet))
    request_couple = sdk_get_replies(looper, [request_couple], total_timeout)[0]
    with pytest.raises(AssertionError):
        sdk_check_reply(request_couple)


# !!! CHECK IF THIS TEST WORKS !!!
def testStewardsCanBeAddedOnlyTillAThresholdIsReached(looper, tconf,
                                                      txnPoolNodeSet,
                                                      tdirWithPoolTxns,
                                                      poolTxnStewardData,
                                                      steward1, stewardWallet):
    addNewClient(STEWARD, looper, steward1, stewardWallet, "testSteward3")

    sendAddNewClient(STEWARD, "testSteward4", steward1, stewardWallet)
    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, steward1,
                             'New stewards cannot be added by other '
                             'stewards as there are already {} '
                             'stewards in the system'.format(
                                 tconf.stewardThreshold),
                             node.clientstack.name)
