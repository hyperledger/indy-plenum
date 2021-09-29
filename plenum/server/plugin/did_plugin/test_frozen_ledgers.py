import pytest
from plenum.test.plugin.demo_plugin.constants import AUCTION_LEDGER_ID

from plenum.test.freeze_ledgers.helper import sdk_send_freeze_ledgers, sdk_get_frozen_ledgers
from plenum.test.helper import freshness

from plenum.common.constants import DATA
from plenum.test.freshness.helper import check_freshness_updated_for_ledger


from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_send_freeze_ledgers(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee):
    ledger_to_remove = AUCTION_LEDGER_ID

    # check that the config state doesn't contain frozen ledgers records
    result = sdk_get_frozen_ledgers(looper, sdk_pool_handle,
                                    sdk_wallet_trustee)[1]["result"][DATA]
    assert result is None

    # add to the config state a frozen ledgers record with an empty list
    sdk_send_freeze_ledgers(
        looper, sdk_pool_handle,
        [sdk_wallet_trustee],
        []
    )

    # check that the config state contains a frozen ledgers record with an empty list
    result = sdk_get_frozen_ledgers(looper, sdk_pool_handle,
                                    sdk_wallet_trustee)[1]["result"][DATA]
    assert len(result) == 0

    # add to the config state a frozen ledgers record with AUCTION ledger
    looper.run(eventually(
        check_freshness_updated_for_ledger, txnPoolNodeSet, ledger_to_remove,
        timeout=3 * FRESHNESS_TIMEOUT)
    )
    sdk_send_freeze_ledgers(
        looper, sdk_pool_handle,
        [sdk_wallet_trustee],
        [ledger_to_remove]
    )

    # check that the config state contains a frozen ledgers record with AUCTION ledger
    result = sdk_get_frozen_ledgers(looper, sdk_pool_handle,
                                    sdk_wallet_trustee)[1]["result"][DATA]
    assert len(result) == 1
    assert result[str(ledger_to_remove)]["state"]
    assert result[str(ledger_to_remove)]["ledger"]
    assert result[str(ledger_to_remove)]["seq_no"] >= 0

    # add to the config state a frozen ledgers record with an empty list
    sdk_send_freeze_ledgers(
        looper, sdk_pool_handle,
        [sdk_wallet_trustee],
        []
    )

    # check that the frozen ledgers list from the state wasn't cleared by the transaction with empty ledgers' list
    result = sdk_get_frozen_ledgers(looper, sdk_pool_handle,
                                    sdk_wallet_trustee)[1]["result"][DATA]
    assert len(result) == 1
    assert result[str(ledger_to_remove)]["state"]
    assert result[str(ledger_to_remove)]["ledger"]
    assert result[str(ledger_to_remove)]["seq_no"] >= 0
