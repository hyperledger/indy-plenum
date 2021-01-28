import pytest

from plenum.test.freeze_ledgers.helper import sdk_send_freeze_ledgers, sdk_get_frozen_ledgers
from plenum.test.helper import freshness

from plenum.common.constants import DATA, DOMAIN_LEDGER_ID
from plenum.test.freshness.helper import check_freshness_updated_for_ledger


from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_send_freeze_ledgers(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee):
    ledger_to_remove = DOMAIN_LEDGER_ID

    looper.run(eventually(
        check_freshness_updated_for_ledger, txnPoolNodeSet, ledger_to_remove,
        timeout=3 * FRESHNESS_TIMEOUT)
    )
    # op = {TXN_TYPE: "123",
    #       LEDGERS_IDS: [DOMAIN_LEDGER_ID]}
    # req = sdk_sign_and_submit_op(looper, sdk_pool_handle, sdk_wallet_trustee, op)
    # sdk_get_and_check_replies(looper, [req])
    #

    # op = {TXN_TYPE: "124"}
    # req = sdk_sign_and_submit_op(looper, sdk_pool_handle, sdk_wallet_trustee, op)
    # result = sdk_get_and_check_replies(looper, [req])
    sdk_send_freeze_ledgers(
        looper, sdk_pool_handle,
        [sdk_wallet_trustee],
        [ledger_to_remove]
    )

    result = sdk_get_frozen_ledgers(looper, sdk_pool_handle,
                                    sdk_wallet_trustee)[1]["result"][DATA]
    print(result)

    assert result[ledger_to_remove]["state"]
    assert result[ledger_to_remove]["ledger"]
    assert result[ledger_to_remove]["seq_no"] >= 0
