from typing import Optional

import pytest

from plenum.common.constants import REPLY, TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, TXN_METADATA, \
    TXN_METADATA_TIME
from plenum.common.util import randomString
from plenum.test.txn_author_agreement.helper import sdk_get_txn_author_agreement, taa_digest, \
    sdk_send_txn_author_agreement

TEXT_V1 = randomString(1024)
V1 = randomString(16)
DIGEST_V1 = taa_digest(TEXT_V1, V1)
TIMESTAMP_V1 = None  # type: Optional[int]

TEXT_V2 = randomString(1024)
V2 = randomString(16)
DIGEST_V2 = taa_digest(TEXT_V2, V2)
TIMESTAMP_V2 = None  # type: Optional[int]


@pytest.fixture(scope='module')
def nodeSetWithTaa(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_trustee):
    global TIMESTAMP_V1, TIMESTAMP_V2

    reply = sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, TEXT_V1, V1)
    TIMESTAMP_V1 = reply[1]['result'][TXN_METADATA][TXN_METADATA_TIME]

    looper.runFor(4)  # Make sure we have long enough gap between updates
    reply = sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, TEXT_V2, V2)
    TIMESTAMP_V2 = reply[1]['result'][TXN_METADATA][TXN_METADATA_TIME]

    return txnPoolNodeSet


def test_get_txn_author_agreement_works_on_clear_state(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY
    assert reply['result']['data'] is None


def test_get_txn_author_agreement_returns_latest_taa_by_default(looper, nodeSetWithTaa,
                                                                sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V2


def test_get_txn_author_agreement_can_return_taa_for_old_version(looper, nodeSetWithTaa,
                                                                 sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=V1)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V1
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V1


def test_get_txn_author_agreement_can_return_taa_for_current_version(looper, nodeSetWithTaa,
                                                                     sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=V2)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V2


def test_get_txn_author_agreement_doesnt_return_taa_for_nonexistent_version(looper, nodeSetWithTaa,
                                                                            sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=randomString(16))[1]
    assert reply['op'] == REPLY
    assert reply['result']['data'] is None


def test_get_txn_author_agreement_can_return_taa_for_old_digest(looper, nodeSetWithTaa,
                                                                sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=DIGEST_V1)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V1
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V1


def test_get_txn_author_agreement_can_return_taa_for_current_digest(looper, nodeSetWithTaa,
                                                                    sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=DIGEST_V2)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V2


def test_get_txn_author_agreement_doesnt_return_taa_for_nonexistent_digest(looper, nodeSetWithTaa,
                                                                           sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=randomString(16))[1]
    assert reply['op'] == REPLY
    assert reply['result']['data'] is None


def test_get_txn_author_agreement_can_return_taa_for_old_ts(looper, nodeSetWithTaa,
                                                            sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         timestamp=TIMESTAMP_V2 - 2)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V1
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V1


def test_get_txn_author_agreement_can_return_taa_for_fresh_ts(looper, nodeSetWithTaa,
                                                              sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         timestamp=TIMESTAMP_V2 + 2)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V2


def test_get_txn_author_agreement_doesnt_return_taa_when_it_didnt_exist(looper, nodeSetWithTaa,
                                                                        sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         timestamp=TIMESTAMP_V1 - 2)[1]
    assert reply['op'] == REPLY
    assert reply['result']['data'] is None
