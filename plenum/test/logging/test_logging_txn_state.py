import functools
import json

import pytest

from plenum.common.exceptions import RequestRejectedException
from plenum.common.util import randomString
from stp_core.loop.eventually import eventually

from plenum.common.constants import DOMAIN_LEDGER_ID, STEWARD_STRING

from plenum.test.pool_transactions.conftest import looper
from plenum.test.pool_transactions.helper import prepare_nym_request, \
    sdk_sign_and_send_prepared_request
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check, sdk_get_and_check_replies


ERORR_MSG = "something went wrong"


def testLoggingTxnStateForValidRequest(
        looper, logsearch, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_client):
    logsPropagate, _ = logsearch(
        levels=['INFO'], files=['propagator.py'],
        funcs=['propagate'], msgs=['propagating.*request.*from client']
    )

    logsOrdered, _ = logsearch(
        levels=['INFO'], files=['replica.py'],
        funcs=['order_3pc_key'], msgs=['ordered batch request']
    )

    logsCommited, _ = logsearch(
        levels=['INFO'], files=['node.py'],
        funcs=['executeBatch'], msgs=['committed batch request']
    )

    reqs = sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                     sdk_wallet_client, 1)
    req, _ = reqs[0]

    reqId = str(req['reqId'])
    assert any(reqId in record.getMessage() for record in logsPropagate)
    assert any(reqId in record.getMessage() for record in logsOrdered)
    assert any(reqId in record.getMessage() for record in logsCommited)


def testLoggingTxnStateForInvalidRequest(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, logsearch):
    logsPropagate, _ = logsearch(
        levels=['INFO'], files=['propagator.py'],
        funcs=['propagate'], msgs=['propagating.*request.*from client']
    )

    logsReject, _ = logsearch(
        levels=['WARNING'], files=['replica.py'],
        funcs=['processReqDuringBatch'],
        msgs=['encountered exception.*while processing.*will reject']
    )

    seed = randomString(32)
    wh, _ = sdk_wallet_client

    nym_request, _ = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_client, seed,
                            "name", STEWARD_STRING))

    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_client,
                                                        sdk_pool_handle, nym_request)

    with pytest.raises(RequestRejectedException) as e:
        sdk_get_and_check_replies(looper, [request_couple])

    assert 'Only Steward is allowed to do these transactions' in e._excinfo[1].args[0]

    reqId = str(json.loads(nym_request)['reqId'])
    assert any(reqId in record.getMessage() for record in logsPropagate)
    assert any(reqId in record.getMessage() for record in logsReject)


def testLoggingTxnStateWhenCommitFails(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, logsearch):
    logsPropagate, _ = logsearch(
        levels=['INFO'], files=['propagator.py'],
        funcs=['propagate'], msgs=['propagating.*request.*from client']
    )

    logsOrdered, _ = logsearch(
        levels=['INFO'], files=['replica.py'],
        funcs=['order_3pc_key'], msgs=['ordered batch request']
    )

    logsCommitFail, _ = logsearch(
        levels=['WARNING'], files=['node.py'],
        funcs=['executeBatch'], msgs=['commit failed for batch request']
    )

    seed = randomString(32)
    wh, _ = sdk_wallet_steward

    nym_request, _ = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_steward, seed,
                            "name", None))

    sdk_sign_and_send_prepared_request(looper, sdk_wallet_steward,
                                       sdk_pool_handle, nym_request)

    class SomeError(Exception):
        pass

    def commitPatched(node, commitOrig, *args, **kwargs):
        req_handler = node.get_req_handler(ledger_id=DOMAIN_LEDGER_ID)
        req_handler.commit = commitOrig
        raise SomeError(ERORR_MSG)

    excCounter = 0

    def executeBatchPatched(node, executeBatchOrig, *args, **kwargs):
        nonlocal excCounter
        try:
            executeBatchOrig(*args, **kwargs)
        except SomeError:
            excCounter += 1
            node.executeBatch = executeBatchOrig
            pass

    def checkSufficientExceptionsHappend():
        assert excCounter == len(txnPoolNodeSet)
        return

    for node in txnPoolNodeSet:
        req_handler = node.get_req_handler(ledger_id=DOMAIN_LEDGER_ID)
        req_handler.commit = functools.partial(
            commitPatched, node, req_handler.commit
        )
        node.executeBatch = functools.partial(
            executeBatchPatched, node, node.executeBatch
        )

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(
        eventually(checkSufficientExceptionsHappend,
                   retryWait=1, timeout=timeout))

    reqId = str(json.loads(nym_request)['reqId'])
    assert any(reqId in record.getMessage() for record in logsPropagate)
    assert any(reqId in record.getMessage() for record in logsOrdered)
    assert any(reqId in record.getMessage() for record in logsCommitFail)
    assert any(ERORR_MSG in record.getMessage() for record in logsCommitFail)
