import pytest
import json

from indy.ledger import build_txn_author_agreement_request

from plenum.common.exceptions import RequestNackedException, RequestRejectedException
from plenum.common.types import OPERATION, f

from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request
from .helper import (
    sdk_send_txn_author_agreement,
    gen_random_txn_author_agreement,
    sdk_send_txn_author_agreement_disable)


def test_send_valid_txn_author_agreement_succeeds_and_disable(
        set_txn_author_agreement_aml, set_txn_author_agreement, get_txn_author_agreement,
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee
):
    set_txn_author_agreement()
    sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet_trustee)
    assert get_txn_author_agreement() is None


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
