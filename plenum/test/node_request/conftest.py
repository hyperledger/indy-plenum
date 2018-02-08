import operator
import importlib
import inspect
import itertools
import logging
import os
import shutil
import re
import warnings
import json
from contextlib import ExitStack
from copy import copy
from functools import partial
import time
from typing import Dict, Any

from indy.pool import create_pool_ledger_config, open_pool_ledger, close_pool_ledger
from indy.wallet import create_wallet, open_wallet, close_wallet
from indy.signus import create_and_store_my_did
from indy.ledger import sign_and_submit_request, sign_request, submit_request, build_nym_request

from ledger.genesis_txn.genesis_txn_file_util import create_genesis_txn_init_ledger
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.signer_did import DidSigner
from plenum.common.signer_simple import SimpleSigner
from plenum.test import waits
from plenum.common.util import getNoInstances

import gc
import pip
import pytest
import plenum.config as plenum_config
import plenum.server.general_config.ubuntu_platform_config as platform_config
from plenum.common.keygen_utils import initNodeKeysForBothStacks, init_bls_keys
from plenum.test.greek import genNodeNames
from plenum.test.grouped_load_scheduling import GroupedLoadScheduling
from plenum.test.node_catchup.helper import ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet
from stp_core.common.logging.handlers import TestingHandler
from stp_core.crypto.util import randomSeed
from stp_core.network.port_dispenser import genHa
from stp_core.types import HA
from _pytest.recwarn import WarningsRecorder

from plenum.common.config_util import getConfig
from stp_core.loop.eventually import eventually
from plenum.common.exceptions import BlowUp
from stp_core.common.log import getlogger, Logger
from stp_core.loop.looper import Looper, Prodable
from plenum.common.constants import TXN_TYPE, DATA, NODE, ALIAS, CLIENT_PORT, \
    CLIENT_IP, NODE_PORT, NYM, CLIENT_STACK_SUFFIX, PLUGIN_BASE_DIR_PATH, ROLE, \
    STEWARD, TARGET_NYM, VALIDATOR, SERVICES, NODE_IP, BLS_KEY, VERKEY, TRUSTEE
from plenum.common.txn_util import getTxnOrderedFields
# from plenum.common.types import PLUGIN_TYPE_STATS_CONSUMER, f
# from plenum.common.util import getNoInstances, getMaxFailures
from plenum.server.notifier_plugin_manager import PluginManager
from plenum.test.helper import randomOperation, \
    checkReqAck, checkLastClientReqForNode, waitForSufficientRepliesForRequests, \
    waitForViewChange, requestReturnedToNode, randomText, \
    mockGetInstalledDistributions, mockImportModule, chk_all_funcs, \
    create_new_test_node, sdk_send_random_request, sdk_json_to_request_object
from plenum.test.node_request.node_request_helper import checkPrePrepared, \
     checkPropagated, checkPrepared, checkCommitted
from plenum.test.plugin.helper import getPluginPath
from plenum.test.test_client import genTestClient, TestClient
from plenum.test.test_node import TestNode, TestNodeSet, Pool, \
    checkNodesConnected, ensureElectionsDone, genNodeReg
from plenum.common.config_helper import PConfigHelper, PNodeConfigHelper
from plenum.test.conftest import getValueFromModule,_tdir, _tconf, \
    _general_conf_tdir, _client_tdir
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, changesRequest
from plenum.test.node_request.node_request_helper import checkCommitted


from plenum.test.pool_transactions.conftest import looper

Logger.setLogLevel(logging.NOTSET)
logger = getlogger()


@pytest.fixture(scope="module")
def committed1(looper, txnPoolNodeSet, prepared1, faultyNodes):
    checkCommitted(looper,
                   txnPoolNodeSet,
                   prepared1,
                   range(getNoInstances(len(txnPoolNodeSet))),
                   faultyNodes)
    return prepared1


@pytest.fixture(scope="module")
def prepared1(looper, txnPoolNodeSet, preprepared1, faultyNodes):
    checkPrepared(looper,
                  txnPoolNodeSet,
                  preprepared1,
                  range(getNoInstances(len(txnPoolNodeSet))),
                  faultyNodes)
    return preprepared1


@pytest.fixture(scope="module")
def preprepared1(looper, txnPoolNodeSet, propagated1, faultyNodes):
    checkPrePrepared(looper,
                     txnPoolNodeSet,
                     propagated1,
                     range(getNoInstances(len(txnPoolNodeSet))),
                     faultyNodes)
    return propagated1


@pytest.fixture(scope="module")
def propagated1(looper,
                txnPoolNodeSet,
                reqAcked1,
                faultyNodes):
    checkPropagated(looper, txnPoolNodeSet, reqAcked1, faultyNodes)
    return reqAcked1


@pytest.fixture(scope="module")
def reqAcked1(looper, txnPoolNodeSet, sent1, faultyNodes):

    numerOfNodes = len(txnPoolNodeSet)

    # Wait until request received by all nodes
    propTimeout = waits.expectedClientToPoolRequestDeliveryTime(numerOfNodes)
    coros = [partial(checkLastClientReqForNode, node, sent1)
             for node in txnPoolNodeSet]
    # looper.run(eventuallyAll(*coros,
    #                          totalTimeout=propTimeout,
    #                          acceptableFails=faultyNodes))
    chk_all_funcs(looper, coros, acceptable_fails=faultyNodes,
                  timeout=propTimeout)

    #TODO: check that sdk response fail when we don't have quorum for answer
    #TODO: check sdk timeout for failed submit (if it more than 90 seconds in evilNodes fixture)

    # # Wait until sufficient number of acks received
    # coros2 = [
    #     partial(
    #         checkReqAck,
    #         client1,
    #         node,
    #         sent1.identifier,
    #         sent1.reqId) for node in txnPoolNodeSet]
    # ackTimeout = waits.expectedReqAckQuorumTime()
    # # looper.run(eventuallyAll(*coros2,
    # #                          totalTimeout=ackTimeout,
    # #                          acceptableFails=faultyNodes))
    # chk_all_funcs(looper, coros2, acceptable_fails=faultyNodes,
    #               timeout=ackTimeout)
    return sent1

@pytest.fixture(scope="module")
def sent1(looper, sdk_pool_handle,
          sdk_wallet_client):
    json_request = sdk_send_random_request(
        looper, sdk_pool_handle, sdk_wallet_client)[0]
    request = sdk_json_to_request_object(json_request)
    return request


@pytest.fixture(scope="module")
def noRetryReq(tconf, request):
    oldRetryAck = tconf.CLIENT_MAX_RETRY_ACK
    oldRetryReply = tconf.CLIENT_MAX_RETRY_REPLY
    tconf.CLIENT_MAX_RETRY_ACK = 0
    tconf.CLIENT_MAX_RETRY_REPLY = 0

    def reset():
        tconf.CLIENT_MAX_RETRY_ACK = oldRetryAck
        tconf.CLIENT_MAX_RETRY_REPLY = oldRetryReply

    request.addfinalizer(reset)
    return tconf