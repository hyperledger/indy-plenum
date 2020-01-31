from typing import Optional

import pytest
from common.serializers.json_serializer import JsonSerializer

from plenum.common.constants import REPLY, TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_METADATA_TIME, TXN_METADATA_SEQ_NO, TXN_AUTHOR_AGREEMENT_DIGEST, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, \
    TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, TXN_METADATA
from plenum.common.util import randomString, get_utc_epoch
from plenum.test.delayers import req_delay
from plenum.test.stasher import delay_rules
from plenum.test.txn_author_agreement.helper import sdk_get_txn_author_agreement, taa_digest, \
    sdk_send_txn_author_agreement, check_state_proof

TEXT_V1 = randomString(1024)
V1 = randomString(16)
DIGEST_V1 = taa_digest(TEXT_V1, V1)
RATIFIED_V1 = None  # type: Optional[int]
TIMESTAMP_V1 = None  # type: Optional[int]

TEXT_V2 = randomString(1024)
V2 = randomString(16)
DIGEST_V2 = taa_digest(TEXT_V2, V2)
RATIFIED_V2 = None  # type: Optional[int]
TIMESTAMP_V2 = None  # type: Optional[int]


@pytest.fixture(scope='module')
def nodeSetWithTaaAlwaysResponding(txnPoolNodeSet, set_txn_author_agreement_aml, looper, sdk_pool_handle,
                                   sdk_wallet_trustee):
    global RATIFIED_V1, RATIFIED_V2
    global TIMESTAMP_V1, TIMESTAMP_V2

    looper.runFor(3)  # Make sure we have long enough gap between updates
    RATIFIED_V1 = get_utc_epoch() - 30
    reply = sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, V1, TEXT_V1, RATIFIED_V1)
    TIMESTAMP_V1 = reply[1]['result'][TXN_METADATA][TXN_METADATA_TIME]

    looper.runFor(3)  # Make sure we have long enough gap between updates
    RATIFIED_V2 = get_utc_epoch() - 30
    reply = sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, V2, TEXT_V2, RATIFIED_V2)
    TIMESTAMP_V2 = reply[1]['result'][TXN_METADATA][TXN_METADATA_TIME]

    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, V1, retired=TIMESTAMP_V1)
    return txnPoolNodeSet


@pytest.fixture(scope='function', params=['all_responding', 'one_responding'])
def nodeSetWithTaa(request, nodeSetWithTaaAlwaysResponding):
    if request.param == 'all_responding':
        yield nodeSetWithTaaAlwaysResponding
    else:
        stashers = [node.clientIbStasher for node in nodeSetWithTaaAlwaysResponding[1:]]
        with delay_rules(stashers, req_delay()):
            yield nodeSetWithTaaAlwaysResponding


def taa_value(result, text, version, digest, retired=None, ratified=None):
    if ratified is None:
        ratified = result[TXN_METADATA_TIME]
    value = {
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version,
            TXN_AUTHOR_AGREEMENT_DIGEST: digest,
            TXN_AUTHOR_AGREEMENT_RATIFICATION_TS: ratified
        }
    if retired:
        value[TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] = retired
    return JsonSerializer().serialize({
        "val": value,
        "lsn": result[TXN_METADATA_SEQ_NO],
        "lut": result[TXN_METADATA_TIME]
    })


def test_get_txn_author_agreement_returns_latest_taa_by_default(looper, set_txn_author_agreement_aml, nodeSetWithTaa,
                                                                sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V2
    assert result['data'][TXN_AUTHOR_AGREEMENT_DIGEST] == DIGEST_V2
    check_state_proof(result, '2:latest', DIGEST_V2)


def test_get_txn_author_agreement_can_return_taa_for_old_version(looper, nodeSetWithTaa,
                                                                 sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=V1)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V1
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V1
    assert result['data'][TXN_AUTHOR_AGREEMENT_DIGEST] == DIGEST_V1
    check_state_proof(result, '2:v:{}'.format(V1), DIGEST_V1)


def test_get_txn_author_agreement_can_return_taa_for_current_version(looper, nodeSetWithTaa,
                                                                     sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=V2)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V2
    check_state_proof(result, '2:v:{}'.format(V2), DIGEST_V2)


def test_get_txn_author_agreement_doesnt_return_taa_for_nonexistent_version(looper, nodeSetWithTaa,
                                                                            sdk_pool_handle, sdk_wallet_client):
    invalid_version = randomString(16)
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         version=invalid_version)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'] is None
    check_state_proof(result, '2:v:{}'.format(invalid_version), None)


def test_get_txn_author_agreement_can_return_taa_for_old_digest(looper, nodeSetWithTaa,
                                                                sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=DIGEST_V1)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V1
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V1
    assert result['data'][TXN_AUTHOR_AGREEMENT_DIGEST] == DIGEST_V1
    assert result['data'][TXN_AUTHOR_AGREEMENT_RETIREMENT_TS]
    check_state_proof(result, '2:d:{}'.format(DIGEST_V1), taa_value(result, TEXT_V1, V1, DIGEST_V1,
                                                                    retired=TIMESTAMP_V1,
                                                                    ratified=RATIFIED_V1))


def test_get_txn_author_agreement_can_return_taa_for_current_digest(looper, nodeSetWithTaa,
                                                                    sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=DIGEST_V2)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V2
    assert result['data'][TXN_AUTHOR_AGREEMENT_DIGEST] == DIGEST_V2
    check_state_proof(result, '2:d:{}'.format(DIGEST_V2), taa_value(result, TEXT_V2, V2, DIGEST_V2,
                                                                    ratified=RATIFIED_V2))


def test_get_txn_author_agreement_doesnt_return_taa_for_nonexistent_digest(looper, nodeSetWithTaa,
                                                                           sdk_pool_handle, sdk_wallet_client):
    invalid_digest = randomString(16)
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         digest=invalid_digest)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'] is None
    check_state_proof(result, '2:d:{}'.format(invalid_digest), None)


def test_get_txn_author_agreement_can_return_taa_for_old_ts(looper, nodeSetWithTaa,
                                                            sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         timestamp=TIMESTAMP_V2 - 2)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V1
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V1
    assert result['data'][TXN_AUTHOR_AGREEMENT_DIGEST] == DIGEST_V1
    check_state_proof(result, '2:latest', DIGEST_V1)


def test_get_txn_author_agreement_can_return_taa_for_fresh_ts(looper, nodeSetWithTaa,
                                                              sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         timestamp=TIMESTAMP_V2 + 2)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_TEXT] == TEXT_V2
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V2
    assert result['data'][TXN_AUTHOR_AGREEMENT_DIGEST] == DIGEST_V2
    check_state_proof(result, '2:latest', DIGEST_V2)


def test_get_txn_author_agreement_doesnt_return_taa_when_it_didnt_exist(looper, nodeSetWithTaa,
                                                                        sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client,
                                         timestamp=TIMESTAMP_V1 - 2)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'] is None
    check_state_proof(result, '2:latest', None)
