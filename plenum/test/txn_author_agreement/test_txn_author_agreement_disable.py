import pytest

from plenum.common.constants import DATA, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, \
    TXN_TIME
from plenum.common.exceptions import RequestRejectedException
from plenum.common.types import f
from plenum.common.util import get_utc_epoch, randomString
from .helper import sdk_send_txn_author_agreement_disable, sdk_get_txn_author_agreement, sdk_send_txn_author_agreement


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

    reply = sdk_get_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet_trustee, version=taa1.version)[1]
    result = reply[f.RESULT.nm]
    assert result[DATA][TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] == retirement_ts


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


@pytest.mark.parametrize('retired_offset', [300, -300, -900, None])
def test_cannot_change_retirement_ts_of_latest_taa_after_disable_all(looper, set_txn_author_agreement_aml,
                                                                     sdk_pool_handle, sdk_wallet_trustee,
                                                                     retired_offset):
    # Write random TAA
    version, text, ratified = randomString(16), randomString(1024), get_utc_epoch() - 600
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  version=version,
                                  text=text,
                                  ratified=ratified)

    # Disable all TAAs
    sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet_trustee)

    # Make sure we cannot change its retirement date
    with pytest.raises(RequestRejectedException):
        retired = get_utc_epoch() + retired_offset if retired_offset is not None else None
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                      version=version, retired=retired)


@pytest.mark.parametrize('retired_offset', [300, -300, -900, None])
def test_cannot_change_retirement_ts_of_non_latest_taa_after_disable_all(looper, set_txn_author_agreement_aml,
                                                                         sdk_pool_handle, sdk_wallet_trustee,
                                                                         retired_offset):
    version_1, text_1, ratified_1 = randomString(16), randomString(1024), get_utc_epoch() - 600
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  version=version_1,
                                  text=text_1,
                                  ratified=ratified_1)

    version_2, text_2, ratified_2 = randomString(16), randomString(1024), get_utc_epoch() - 600
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  version=version_2,
                                  text=text_2,
                                  ratified=ratified_2)

    # Disable all TAAs
    sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet_trustee)

    # Make sure we cannot change its retirement date
    with pytest.raises(RequestRejectedException):
        retired = get_utc_epoch() + retired_offset if retired_offset is not None else None
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                      version=version_1, retired=retired)
