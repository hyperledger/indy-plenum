import pytest

from plenum.common.constants import REPLY, TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION
from plenum.common.util import randomString
from plenum.test.txn_author_agreement.helper import sdk_get_txn_author_agreement, taa_digest, \
    sdk_send_txn_author_agreement

TEXT_V1 = randomString(1024)
V1 = randomString(16)
DIGEST_V1 = taa_digest(TEXT_V1, V1)

TEXT_V2 = randomString(1024)
V2 = randomString(16)
DIGEST_V2 = taa_digest(TEXT_V2, V2)


@pytest.fixture(scope='module')
def nodeSetWithTaa(setup, txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_trustee):
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, TEXT_V1, V1)
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, TEXT_V2, V2)
    return txnPoolNodeSet


def test_get_txn_author_agreement_works_on_clear_state(looper, setup, txnPoolNodeSet, sdk_pool_handle,
                                                       sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY
    assert reply['result']['data'] is None


def test_get_txn_author_agreement_returns_latest_taa_by_default(looper, setup, nodeSetWithTaa,
                                                                sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V2


def test_get_txn_author_agreement_can_return_taa_for_old_version(looper, setup, nodeSetWithTaa,
                                                                 sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=V1)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V1
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V1


def test_get_txn_author_agreement_can_return_taa_for_current_version(looper, setup, nodeSetWithTaa,
                                                                     sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=V2)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V2


def test_get_txn_author_agreement_doesnt_return_taa_for_nonexistent_version(looper, setup, nodeSetWithTaa,
                                                                            sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=randomString(16))[1]
    assert reply['op'] == REPLY
    assert reply['result']['data'] is None


def test_get_txn_author_agreement_can_return_taa_for_old_digest(looper, setup, nodeSetWithTaa,
                                                                sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=DIGEST_V1)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V1
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V1


def test_get_txn_author_agreement_can_return_taa_for_current_digest(looper, setup, nodeSetWithTaa,
                                                                    sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=DIGEST_V2)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == V2


def test_get_txn_author_agreement_doesnt_return_taa_for_nonexistent_digest(looper, setup, nodeSetWithTaa,
                                                                           sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=randomString(16))[1]
    assert reply['op'] == REPLY
    assert reply['result']['data'] is None
