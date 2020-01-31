import json
from random import randint
from typing import Optional

import pytest
from indy.ledger import build_acceptance_mechanisms_request
from plenum.common.exceptions import RequestNackedException

from plenum.common.types import OPERATION, f
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request

from common.serializers.json_serializer import JsonSerializer

from plenum.common.constants import REPLY, TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, TXN_METADATA, \
    TXN_METADATA_TIME, TXN_METADATA_SEQ_NO, CONFIG_LEDGER_ID, AML_VERSION, AML, AML_CONTEXT, TXN_TYPE, \
    GET_TXN_AUTHOR_AGREEMENT_AML, CURRENT_PROTOCOL_VERSION, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, TXN_AUTHOR_AGREEMENT_DIGEST
from plenum.common.util import randomString
from plenum.test.delayers import req_delay
from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.stasher import delay_rules
from plenum.test.txn_author_agreement.helper import check_state_proof, sdk_get_taa_aml, sdk_send_txn_author_agreement

V1 = randomString(16)
AML1 = {'Nice way': 'very good way to accept agreement'}
CONTEXT1 = randomString()
TIMESTAMP_V1 = None  # type: Optional[int]

V2 = randomString(16)
AML2 = {'Another good way': 'one more good way to accept agreement'}
CONTEXT2 = randomString()
TIMESTAMP_V2 = None  # type: Optional[int]


def send_aml_request(looper, sdk_wallet_trustee, sdk_pool_handle, version, aml, context):
    req = looper.loop.run_until_complete(build_acceptance_mechanisms_request(
        sdk_wallet_trustee[1],
        aml,
        version, context))
    req = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, req)
    return sdk_get_and_check_replies(looper, [req])[0]


@pytest.fixture(scope='module')
def nodeSetWithTaaAlwaysResponding(txnPoolNodeSet, looper, sdk_pool_handle,
                                   sdk_wallet_trustee):
    global TIMESTAMP_V1, TIMESTAMP_V2

    # Force signing empty config state
    txnPoolNodeSet[0].master_replica._ordering_service._do_send_3pc_batch(ledger_id=CONFIG_LEDGER_ID)

    looper.runFor(3)  # Make sure we have long enough gap between updates
    reply = send_aml_request(looper, sdk_wallet_trustee, sdk_pool_handle, version=V1, aml=json.dumps(AML1),
                             context=CONTEXT1)
    TIMESTAMP_V1 = reply[1]['result'][TXN_METADATA][TXN_METADATA_TIME]

    looper.runFor(3)  # Make sure we have long enough gap between updates
    reply = send_aml_request(looper, sdk_wallet_trustee, sdk_pool_handle, version=V2, aml=json.dumps(AML2),
                             context=CONTEXT2)
    TIMESTAMP_V2 = reply[1]['result'][TXN_METADATA][TXN_METADATA_TIME]

    return txnPoolNodeSet


@pytest.fixture(scope='function', params=['all_responding', 'one_responding'])
def nodeSetWithTaa(request, nodeSetWithTaaAlwaysResponding):
    if request.param == 'all_responding':
        yield nodeSetWithTaaAlwaysResponding
    else:
        stashers = [node.clientIbStasher for node in nodeSetWithTaaAlwaysResponding[1:]]
        with delay_rules(stashers, req_delay()):
            yield nodeSetWithTaaAlwaysResponding


def taa_value(result, text, version, digest, retired=False):
    value = {
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version,
            TXN_AUTHOR_AGREEMENT_DIGEST: digest
        }
    if retired:
        value[TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] = retired
    return JsonSerializer().serialize({
        "val": value,
        "lsn": result[TXN_METADATA_SEQ_NO],
        "lut": result[TXN_METADATA_TIME]
    })


def taa_aml_value(result, version, aml, context):
    return JsonSerializer().serialize({
        "val": {
            AML_VERSION: version,
            AML: aml,
            AML_CONTEXT: context
        },
        "lsn": result[TXN_METADATA_SEQ_NO],
        "lut": result[TXN_METADATA_TIME]
    })


def test_get_taa_aml_static_validation_fails(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    req = {
        OPERATION: {
            TXN_TYPE: GET_TXN_AUTHOR_AGREEMENT_AML,
            'timestamp': randint(1, 2147483647),
            'version': randomString()
        },
        f.IDENTIFIER.nm: sdk_wallet_client[1],
        f.REQ_ID.nm: randint(1, 2147483647),
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_client, sdk_pool_handle, json.dumps(req))
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [rep])
    e.match('cannot be used in GET_TXN_AUTHOR_AGREEMENT_AML request together')


def test_get_taa_aml_works_on_clear_state(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY
    assert reply['result']['data'] is None


def test_get_taa_aml_returns_latest_taa_by_default(looper, nodeSetWithTaa,
                                                   sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][AML_VERSION] == V2
    check_state_proof(result, '3:latest', taa_aml_value(result, V2, AML2, CONTEXT2))


def test_get_taa_aml_can_return_taa_for_old_version(looper, nodeSetWithTaa,
                                                    sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet_client, version=V1)[1]

    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V1
    check_state_proof(result, '3:v:{}'.format(V1), taa_aml_value(result, V1, AML1, CONTEXT1))


def test_get_taa_aml_can_return_taa_for_current_version(looper, nodeSetWithTaa,
                                                        sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet_client, version=V2)[1]

    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V2
    check_state_proof(result, '3:v:{}'.format(V2), taa_aml_value(result, V2, AML2, CONTEXT2))


def test_get_taa_aml_doesnt_return_taa_for_nonexistent_version(looper, nodeSetWithTaa,
                                                               sdk_pool_handle, sdk_wallet_client):
    invalid_version = randomString(16)
    reply = sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet_client,
                            version=invalid_version)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'] is None
    check_state_proof(result, '3:v:{}'.format(invalid_version), None)


def test_get_taa_aml_can_return_taa_aml_for_old_ts(looper, nodeSetWithTaa,
                                                   sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet_client,
                            timestamp=TIMESTAMP_V2 - 2)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V1
    check_state_proof(result, '3:latest', taa_aml_value(result, V1, AML1, CONTEXT1))


def test_get_taa_aml_can_return_taa_aml_for_fresh_ts(looper, nodeSetWithTaa,
                                                     sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet_client,
                            timestamp=TIMESTAMP_V2 + 2)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'][TXN_AUTHOR_AGREEMENT_VERSION] == V2
    check_state_proof(result, '3:latest', taa_aml_value(result, V2, AML2, CONTEXT2))


# TODO: Change to nodeSetWithTaa when SDK will support this case
def test_get_taa_aml_doesnt_return_taa_aml_when_it_didnt_exist(looper, nodeSetWithTaaAlwaysResponding,
                                                               sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet_client,
                            timestamp=TIMESTAMP_V1 - 3)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'] is None
    check_state_proof(result, '3:latest', None)
