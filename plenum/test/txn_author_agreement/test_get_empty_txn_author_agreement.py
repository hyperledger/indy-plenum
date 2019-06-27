import pytest

from plenum.common.constants import REPLY, CONFIG_LEDGER_ID
from plenum.common.exceptions import RequestNackedException
from plenum.common.util import get_utc_epoch
from plenum.test.delayers import req_delay
from plenum.test.stasher import delay_rules
from plenum.test.txn_author_agreement.helper import sdk_get_txn_author_agreement, check_state_proof

whitelist = ['Unexpected combination of request parameters']

TIMESTAMP_NONE = None


@pytest.fixture(scope='module')
def nodeSetWithoutTaaAlwaysResponding(txnPoolNodeSet, looper):
    global TIMESTAMP_NONE

    # Simulate freshness update
    txnPoolNodeSet[0].master_replica._do_send_3pc_batch(ledger_id=CONFIG_LEDGER_ID)

    looper.runFor(1)  # Make sure we have long enough gap between updates
    TIMESTAMP_NONE = get_utc_epoch()

    return txnPoolNodeSet


@pytest.fixture(scope='function', params=['all_responding', 'one_responding'])
def nodeSetWithoutTaa(request, nodeSetWithoutTaaAlwaysResponding):
    if request.param == 'all_responding':
        yield nodeSetWithoutTaaAlwaysResponding
    else:
        stashers = [node.clientIbStasher for node in nodeSetWithoutTaaAlwaysResponding[1:]]
        with delay_rules(stashers, req_delay()):
            yield nodeSetWithoutTaaAlwaysResponding


@pytest.mark.parametrize(argnames="params, state_key", argvalues=[
    ({}, '2:latest'),
    ({'digest': 'some_digest'}, '2:d:some_digest'),
    ({'version': 'some_version'}, '2:v:some_version'),
    ({'timestamp': TIMESTAMP_NONE}, '2:latest')
])
def test_get_txn_author_agreement_works_on_clear_state(params, state_key, looper, nodeSetWithoutTaa,
                                                       sdk_pool_handle, sdk_wallet_client):
    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client, **params)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    assert result['data'] is None
    check_state_proof(result, state_key, None)


@pytest.mark.parametrize(argnames="params", argvalues=[
    {'digest': 'some_digest', 'version': 'some_version'},
    {'digest': 'some_digest', 'timestamp': 374273},
    {'version': 'some_version', 'timestamp': 374273},
    {'digest': 'some_digest', 'version': 'some_version', 'timestamp': 374273}
])
def test_get_txn_author_agreement_cannot_have_more_than_one_parameter(params, looper, nodeSetWithoutTaa,
                                                                      sdk_pool_handle, sdk_wallet_client):
    with pytest.raises(RequestNackedException) as e:
        sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client, **params)
    assert e.match("GET_TXN_AUTHOR_AGREEMENT request can have at most one "
                   "of the following parameters: version, digest, timestamp")
