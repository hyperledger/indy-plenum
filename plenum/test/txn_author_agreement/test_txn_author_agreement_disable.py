import pytest

from plenum.common.constants import DATA
from plenum.common.exceptions import RequestRejectedException
from plenum.common.types import f
from .helper import sdk_send_txn_author_agreement_disable, sdk_get_txn_author_agreement


def test_send_valid_txn_author_agreement_succeeds_and_disable(
        set_txn_author_agreement_aml, set_txn_author_agreement, get_txn_author_agreement,
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee
):
    taa = set_txn_author_agreement()
    # print(taa)
    sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet_trustee)
    assert get_txn_author_agreement() is None
    reply = sdk_get_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet_trustee, version=taa.version)[1]
    result = reply[f.RESULT.nm][DATA]
    print(result)

    # assert get_txn_author_agreement(version=taa.version) is None


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
