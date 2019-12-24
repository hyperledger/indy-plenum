import pytest

from plenum.common.constants import DATA, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, \
    TXN_TIME
from plenum.common.exceptions import RequestRejectedException
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from .helper import sdk_send_txn_author_agreement_disable, sdk_get_txn_author_agreement


def test_send_valid_txn_author_agreement_succeeds_and_disable(
        set_txn_author_agreement_aml, set_txn_author_agreement, get_txn_author_agreement,
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee
):
    ratified = get_utc_epoch() - 600
    taa1 = set_txn_author_agreement(ratified=ratified)
    taa2 = set_txn_author_agreement()
    retirement_ts = 5
    taa1 = set_txn_author_agreement(taa1.text, taa1.version, retirement_ts, ratified=ratified)
    sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet_trustee)

    reply = sdk_get_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet_trustee)[1]
    assert reply[f.RESULT.nm][DATA] is None

    reply = sdk_get_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet_trustee, version=taa2.version)[1]
    result = reply[f.RESULT.nm]
    assert result[DATA][TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] == result[TXN_TIME]
    #
    # reply = sdk_get_txn_author_agreement(
    #     looper, sdk_pool_handle, sdk_wallet_trustee, version=taa1.version)[1]
    # result = reply[f.RESULT.nm]
    # assert result[DATA][TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] == retirement_ts


def test_send_txn_author_agreement_disable_twice(
        set_txn_author_agreement_aml, set_txn_author_agreement, get_txn_author_agreement,
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee
):
    set_txn_author_agreement()
    sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet_trustee)
    with pytest.raises(
            RequestRejectedException,
            match='Transaction author agreement is already disabled'
    ):
        sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet_trustee)
    assert get_txn_author_agreement() is None
