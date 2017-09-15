import pytest
import json
import os
from random import randint
import base58
import re
import time
import functools


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
from plenum.test.validator_info.helper import load_info

logger = getlogger()


TEST_NODE_NAME = "NewNode"


@pytest.fixture(scope="module")
def test_ledgers():
    return [(POOL_LEDGER_ID, 'pool'), (DOMAIN_LEDGER_ID, 'ledger')]


@pytest.fixture
def steward_client_and_wallet(steward1, stewardWallet):
    return steward1, stewardWallet


def check_ledgers_state(test_ledgers, states, loader):
    latest_info = loader()
    states = [state.name for state in states]
    for ledger_id, ledger_name in test_ledgers:
        assert latest_info['ledgers'][ledger_name] in states


# TODO more cases for states
@pytest.fixture(scope="function")
def check_ledgers_state_fields(
        test_ledgers, info_path,
        latest_info_wait_time, load_latest_info, looper, txnPoolNodeSet,
        tconf, tdir, steward_client_and_wallet):

    steward, stewardWallet = steward_client_and_wallet

    assert test_ledgers

    newStewardName = "NewSteward"
    newNodeName = TEST_NODE_NAME

    logger.debug("Creating new steward {}".format(newStewardName))
    newSteward, newStewardWallet = addNewSteward(
            looper, tdir, steward, stewardWallet, newStewardName)

    logger.debug("Adding new node {} to the pool".format(newNodeName))
    req, nodeIp, nodePort, clientIp, clientPort, sigseed \
        = sendAddNewNode(newNodeName, newSteward, newStewardWallet)
    waitForSufficientRepliesForRequests(looper, newSteward,
                                        requests=[req], fVal=1)

    logger.debug("Creating and starting node {}".format(newNodeName))
    newNode = start_newly_added_node(looper, newNodeName, tdir, sigseed,
                                  (nodeIp, nodePort), (clientIp, clientPort),
                                  tconf, True, None, TestNode)

    # TODO taking into account all nodes
    # (including already connected and synced ones)
    # here seems too pessimistic
    numNodes = len(txnPoolNodeSet) + 1
    logger.debug(
        "Delay consistency proof requests processing "
        "for node {} to ensure that node's ledgers are "
        "in not-synced state".format(newNodeName)
    )

    newNode.nodeIbStasher.delay(
        cpDelay(
            latest_info_wait_time + 1 +
            waits.expectedPoolInterconnectionTime(numNodes)
        )
    )

    check_ledgers_state(
            test_ledgers, (LedgerState.not_synced,), load_latest_info)

    txnPoolNodeSet.append(newNode)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    logger.debug(
        "Reset consistency proof delays and and ensure that ledger "
        "starts syncing (sufficient consistency proofs received)")
    newNode.nodeIbStasher.reset_delays_and_process_delayeds(CONSISTENCY_PROOF)

    waits_catch_up = waits.expectedPoolCatchupTime(numNodes)
    looper.run(
        eventually(
            check_ledgers_state, test_ledgers,
            (LedgerState.syncing, LedgerState.synced),
            functools.partial(load_info, info_path),
            retryWait=.5,
            timeout=waits_catch_up
        )
    )

    # TODO more accurate checks for 'syncing' and 'synced' states only
    # it's unlikely to do that using catch-up delay because ledgers are
    # synced one by one)

    
    # logger.debug(
    #    "Delay catchup reply processing for node {} ".format(newNodeName)
    # )
    # newNode.nodeIbStasher.delay(cr_delay(waits_catch_up))

    #logger.debug(
    #    "Reset catchup reply delays and ensure "
    #    "that all ledgers are synced"
    #)
    #newNode.nodeIbStasher.reset_delays_and_process_delayeds(CATCHUP_REP)
    #ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    #latest_info = load_latest_info()
    #check_ledgers_state(test_ledgers, (LedgerState.synced,),
    #        functools.partial(load_info, info_path))



def test_validator_info_file_ledgers_pool_domain_state_fields_valid(
        check_ledgers_state_fields):
    pass
