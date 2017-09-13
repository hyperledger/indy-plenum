import pytest
import json
import os
from random import randint
import base58
import re
import time


from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

from plenum.common.constants import LedgerState, CONSISTENCY_PROOF, \
        CATCHUP_REP, POOL_LEDGER_ID, DOMAIN_LEDGER_ID

from plenum.test import waits
from plenum.test.conftest import getValueFromModule
from plenum.test.helper import waitForSufficientRepliesForRequests
from plenum.test.delayers import cpDelay, cr_delay
from plenum.test.test_node import TestNode, checkNodesConnected
from plenum.test.node_catchup.helper import check_ledger_state, \
        ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.conftest import looper, steward1, \
    stewardWallet, stewardAndWallet1 # noqa
from plenum.test.pool_transactions.helper import addNewSteward, \
        sendAddNewNode, start_newly_added_node

logger = getlogger()


TEST_NODE_NAME = "NewNode"


@pytest.fixture(scope="module")
def test_ledgers(request):
    return getValueFromModule(request, "TEST_LEDGERS",
            [(POOL_LEDGER_ID, 'pool'), (DOMAIN_LEDGER_ID, 'ledger')])


# TODO more cases for states
@pytest.fixture(scope="function")
def check_ledgers_state_fields(
        test_ledgers,
        latest_info_wait_time, load_latest_info, looper, txnPoolNodeSet,
        tconf, tdir, steward1, stewardWallet):

    assert test_ledgers

    newStewardName = "NewSteward"
    newNodeName = TEST_NODE_NAME

    logger.debug("Creating new steward {}".format(newStewardName))
    newSteward, newStewardWallet = addNewSteward(
            looper, tdir, steward1, stewardWallet, newStewardName)

    logger.debug("Adding new node {} to the pool".format(newNodeName))
    req, nodeIp, nodePort, clientIp, clientPort, sigseed \
        = sendAddNewNode(newNodeName, newSteward, newStewardWallet)
    waitForSufficientRepliesForRequests(looper, newSteward,
                                        requests=[req], fVal=1)

    logger.debug("Creating and starting node {}".format(newNodeName))
    newNode = start_newly_added_node(looper, newNodeName, tdir, sigseed,
                                  (nodeIp, nodePort), (clientIp, clientPort),
                                  tconf, True, None, TestNode)

    logger.debug(
        "Delay consistency proof requests processing "
        "for node {} ".format(newNodeName))
    newNode.nodeIbStasher.delay(cpDelay(latest_info_wait_time))

    logger.debug(
        "Delay catchup reply processing for node {} "
        "so LedgerState does not change".format(newNodeName))
    waits_consistency_proof = waits.expectedPoolConsistencyProof(1)
    newNode.nodeIbStasher.delay(
            cr_delay(latest_info_wait_time * len(test_ledgers)))

    latest_info = load_latest_info()
    assert latest_info['ledgers']['ledger'] == LedgerState.not_synced.name

    txnPoolNodeSet.append(newNode)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    logger.debug(
        "Reset consistency proof delays and and ensure that ledger "
        "starts syncing (sufficient consistency proofs received)")
    newNode.nodeIbStasher.reset_delays_and_process_delayeds(CONSISTENCY_PROOF)

    now = time.time()
    remainingTime = waits_consistency_proof
    for ledger_id, ledger_name in test_ledgers:
        if remainingTime > 0:
            looper.run(eventually(check_ledger_state, newNode, ledger_id,
                                  LedgerState.syncing, retryWait=.5,
                                  timeout=waits_consistency_proof))
        latest_info = load_latest_info()
        assert latest_info['ledgers'][ledger_name] == LedgerState.syncing.name

        _now = time.time()
        remainingTime -= _now - now
        now = _now


    latest_info = load_latest_info()
    assert latest_info['ledgers']['ledger'] == LedgerState.syncing.name

    logger.debug(
        "Reset catchup reply delays and ensure "
        "that all ledgers are synced"
    )
    newNode.nodeIbStasher.reset_delays_and_process_delayeds(CATCHUP_REP)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    latest_info = load_latest_info()
    assert latest_info['ledgers']['pool'] == LedgerState.synced.name
    assert latest_info['ledgers']['ledger'] == LedgerState.synced.name


def test_validator_info_file_ledgers_pool_domain_state_fields_valid(
        check_ledgers_state_fields):
    pass
