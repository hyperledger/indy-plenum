import logging
from statistics import pstdev, mean
from time import perf_counter
from types import MethodType

import math
import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.common.perf_util import get_memory_usage, timeit
from plenum.test.delayers import cr_delay
from plenum.test.test_client import TestClient

from stp_core.loop.eventually import eventually
from plenum.common.types import HA
from stp_core.common.log import getlogger, Logger
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    check_ledger_state
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, buildPoolClientAndWallet
from plenum.test.test_node import checkNodesConnected, TestNode
from plenum.test import waits

# noinspection PyUnresolvedReferences
from plenum.test.node_catchup.conftest import whitelist, \
    nodeCreatedAfterSomeTxns, nodeSetWithNodeAddedAfterSomeTxns, newNodeCaughtUp
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

@pytest.fixture
def logger():
    logger = getlogger()
    old_value = logger.getEffectiveLevel()
    logger.root.setLevel(logging.WARNING)
    yield logger
    logger.root.setLevel(old_value)

# autouse and inject before others in all tests
pytestmark = pytest.mark.usefixtures("logger")

txnCount = 5
TestRunningTimeLimitSec = math.inf


"""
Since these tests expect performance to be of certain level, they can fail and
for now should only be run when a perf check is required, like after a relevant
change in protocol, setting `SkipTests` to False will run tests in this
module
"""
SkipTests = True
skipper = pytest.mark.skipif(SkipTests, reason='Perf optimisations not done')


@pytest.fixture(scope="module")
def disable_node_monitor_config(tconf):
    tconf.unsafe.add('disable_view_change')
    # tconf.unsafe.add('disable_monitor')
    return tconf


@pytest.fixture(scope="module")
def change_checkpoint_freq(tconf):
    tconf.CHK_FREQ = 3


@skipper
def test_node_load(looper, txnPoolNodeSet, tconf,
                   tdirWithPoolTxns, allPluginsPath,
                   poolTxnStewardData, capsys):
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithPoolTxns,
                                              clientClass=TestClient)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())

    client_batches = 150
    txns_per_batch = 25
    for i in range(client_batches):
        s = perf_counter()
        sendReqsToNodesAndVerifySuffReplies(looper, wallet, client,
                                            txns_per_batch,
                                            override_timeout_limit=True)
        with capsys.disabled():
            print('{} executed {} client txns in {:.2f} seconds'.
                  format(i + 1, txns_per_batch, perf_counter() - s))


@skipper
def test_node_load_consistent_time(tconf, change_checkpoint_freq,
                                   disable_node_monitor_config, looper,
                                   txnPoolNodeSet, tdirWithPoolTxns,
                                   allPluginsPath, poolTxnStewardData, capsys):

    # One of the reason memory grows is because spylog grows
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithPoolTxns,
                                              clientClass=TestClient)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())

    client_batches = 300
    txns_per_batch = 25
    time_log = []
    warm_up_batches = 10
    tolerance_factor = 2
    print_detailed_memory_usage = False
    from pympler import tracker
    tr = tracker.SummaryTracker()
    node_methods_to_capture = [TestNode.executeBatch,
                               TestNode.recordAndPropagate,
                               TestNode.domainDynamicValidation,
                               TestNode.domainRequestApplication]
    times = {n.name: {meth.__name__: [] for meth in node_methods_to_capture}
             for n in txnPoolNodeSet}

    for node in txnPoolNodeSet:
        for meth in node_methods_to_capture:
            meth_name = meth.__name__
            patched = timeit(getattr(node, meth_name),
                             times[node.name][meth_name])
            setattr(node, meth_name, patched)

    for i in range(client_batches):
        s = perf_counter()
        sendReqsToNodesAndVerifySuffReplies(looper, wallet, client,
                                            txns_per_batch,
                                            override_timeout_limit=True)
        t = perf_counter() - s
        with capsys.disabled():
            print('{} executed {} client txns in {:.2f} seconds'.
                  format(i + 1, txns_per_batch, t))
            print('--------Memory Usage details start')
            for node in txnPoolNodeSet:
                # print(sys.getsizeof(node))
                print('---Node {}-----'.format(node))
                # print('Requests {}'.format(asizeof.asizeof(node.requests, detail=1)))
                print(
                    get_memory_usage(
                        node,
                        print_detailed_memory_usage,
                        get_only_non_empty=True))
                for r in node.replicas:
                    print('---Replica {}-----'.format(r))
                    print(
                        get_memory_usage(
                            r,
                            print_detailed_memory_usage,
                            get_only_non_empty=True))

            # if i % 3 == 0:
            #     tr.print_diff()
            print('--------Memory Usage details end')
            for node in txnPoolNodeSet:
                for meth in node_methods_to_capture:
                    ts = times[node.name][meth.__name__]
                    print('{} {} {} {}'.format(
                        node, meth.__name__, mean(ts), ts))

        if len(time_log) >= warm_up_batches:
            m = mean(time_log)
            sd = tolerance_factor * pstdev(time_log)
            assert m > t or abs(t - m) <= sd, '{} {}'.format(abs(t - m), sd)
        time_log.append(t)
        # Since client checks inbox for sufficient replies, clear inbox so that
        #  it takes constant time to check replies for each batch
        client.inBox.clear()
        client.txnLog.reset()


@skipper
def test_node_load_after_add(newNodeCaughtUp, txnPoolNodeSet, tconf,
                             tdirWithPoolTxns, allPluginsPath,
                             poolTxnStewardData, looper, client1, wallet1,
                             client1Connected, capsys):
    """
    A node that restarts after some transactions should eventually get the
    transactions which happened while it was down
    :return:
    """
    new_node = newNodeCaughtUp
    logger.debug("Sending requests")

    # Here's where we apply some load
    client_batches = 300
    txns_per_batch = 25
    for i in range(client_batches):
        s = perf_counter()
        sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                            txns_per_batch,
                                            override_timeout_limit=True)
        with capsys.disabled():
            print('{} executed {} client txns in {:.2f} seconds'.
                  format(i + 1, txns_per_batch, perf_counter() - s))

    logger.debug("Starting the stopped node, {}".format(new_node))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])


@skipper
def test_node_load_after_add_then_disconnect(newNodeCaughtUp, txnPoolNodeSet,
                                             tconf, looper, client1, wallet1,
                                             client1Connected,
                                             tdirWithPoolTxns, allPluginsPath,
                                             poolTxnStewardData, capsys):
    """
    A node that restarts after some transactions should eventually get the
    transactions which happened while it was down
    :return:
    """
    new_node = newNodeCaughtUp
    with capsys.disabled():
        print("Stopping node {} with pool ledger size {}".
              format(new_node, new_node.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, new_node)
    looper.removeProdable(new_node)

    client_batches = 80
    txns_per_batch = 10
    for i in range(client_batches):
        s = perf_counter()
        sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                            txns_per_batch,
                                            override_timeout_limit=True)
        with capsys.disabled():
            print('{} executed {} client txns in {:.2f} seconds'.
                  format(i + 1, txns_per_batch, perf_counter() - s))

    with capsys.disabled():
        print("Starting the stopped node, {}".format(new_node))
    nodeHa, nodeCHa = HA(*new_node.nodestack.ha), HA(*new_node.clientstack.ha)
    new_node = TestNode(
        new_node.name,
        basedirpath=tdirWithPoolTxns,
        base_data_dir=tdirWithPoolTxns,
        config=tconf,
        ha=nodeHa,
        cliha=nodeCHa,
        pluginPaths=allPluginsPath)
    looper.add(new_node)
    txnPoolNodeSet[-1] = new_node

    # Delay catchup reply processing so LedgerState does not change
    delay_catchup_reply = 5
    new_node.nodeIbStasher.delay(cr_delay(delay_catchup_reply))
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Make sure ledger starts syncing (sufficient consistency proofs received)
    looper.run(eventually(check_ledger_state, new_node, DOMAIN_LEDGER_ID,
                          LedgerState.syncing, retryWait=.5, timeout=5))

    # Not accurate timeout but a conservative one
    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet)) + \
        2 * delay_catchup_reply
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4],
                         customTimeout=timeout)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])


@skipper
def test_nodestack_contexts_are_discrete(txnPoolNodeSet):
    assert txnPoolNodeSet[0].nodestack.ctx != txnPoolNodeSet[1].nodestack.ctx
    ctx_objs = {n.nodestack.ctx for n in txnPoolNodeSet}
    ctx_underlying = {n.nodestack.ctx.underlying for n in txnPoolNodeSet}
    assert len(ctx_objs) == len(txnPoolNodeSet)
    assert len(ctx_underlying) == len(txnPoolNodeSet)


@skipper
def test_node_load_after_disconnect(looper, txnPoolNodeSet, tconf,
                                    tdirWithPoolTxns, allPluginsPath,
                                    poolTxnStewardData, capsys):

    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithPoolTxns,
                                              clientClass=TestClient)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())

    nodes = txnPoolNodeSet
    x = nodes[-1]

    with capsys.disabled():
        print("Stopping node {} with pool ledger size {}".
              format(x, x.poolManager.txnSeqNo))

    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, x)
    looper.removeProdable(x)

    client_batches = 80
    txns_per_batch = 10
    for i in range(client_batches):
        s = perf_counter()
        sendReqsToNodesAndVerifySuffReplies(looper, wallet, client,
                                            txns_per_batch,
                                            override_timeout_limit=True)
        with capsys.disabled():
            print('{} executed {} client txns in {:.2f} seconds'.
                  format(i + 1, txns_per_batch, perf_counter() - s))

    nodeHa, nodeCHa = HA(*x.nodestack.ha), HA(*x.clientstack.ha)
    newNode = TestNode(x.name, basedirpath=tdirWithPoolTxns, base_data_dir=tdirWithPoolTxns, config=tconf,
                       ha=nodeHa, cliha=nodeCHa, pluginPaths=allPluginsPath)
    looper.add(newNode)
    txnPoolNodeSet[-1] = newNode
    looper.run(checkNodesConnected(txnPoolNodeSet))


@skipper
def test_node_load_after_one_node_drops_all_msgs(
        looper,
        txnPoolNodeSet,
        tconf,
        tdirWithPoolTxns,
        allPluginsPath,
        poolTxnStewardData,
        capsys):

    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithPoolTxns,
                                              clientClass=TestClient)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())

    nodes = txnPoolNodeSet
    x = nodes[-1]

    with capsys.disabled():
        print("Patching node {}".format(x))

    def handleOneNodeMsg(self, wrappedMsg):
        # do nothing with an incoming node message
        pass

    x.handleOneNodeMsg = MethodType(handleOneNodeMsg, x)

    client_batches = 120
    txns_per_batch = 25
    for i in range(client_batches):
        s = perf_counter()
        sendReqsToNodesAndVerifySuffReplies(looper, wallet, client,
                                            txns_per_batch,
                                            override_timeout_limit=True)
        with capsys.disabled():
            print('{} executed {} client txns in {:.2f} seconds'.
                  format(i + 1, txns_per_batch, perf_counter() - s))
