from logging import getLogger

import pytest as pytest

from plenum.test.delayers import cqDelay, cpDelay
from plenum.test.logging.conftest import logsearch
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node

logger = getLogger()


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        old = tconf.CATCHUP_BATCH_SIZE
        tconf.CATCHUP_BATCH_SIZE = 1
        yield tconf
        tconf.CATCHUP_BATCH_SIZE = old


def test_catchup_with_one_slow_node(tdir, tconf,
                                    looper,
                                    txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client,
                                    allPluginsPath,
                                    logsearch):
    '''
    1. Stop the node Delta
    2. Delay ConsProof messages on Alpha, so CatchupReqs will be sent to Beta and Gamma only
    3. Order 9 txns. In sending CatchupReq in a first round only
    nodes [Beta, Gamma] will receive requests for 9 txns in total.
    4. Delay CatchupReq on Beta
    4. Start Delta
    5. Check that all nodes have equality data.
    6. Check that Delta re-ask CatchupRep only once.
    In the second CatchupRep (first re-ask) Delta shouldn't request
    CatchupRep from Beta because it didn't answer early.
    '''
    # Prepare nodes
    lagging_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]

    # Stop one node
    waitNodeDataEquality(looper, lagging_node, *rest_nodes)
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            lagging_node,
                                            stopNode=True)
    looper.removeProdable(lagging_node)

    # Send more requests to active nodes
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, len(rest_nodes) * 3)
    waitNodeDataEquality(looper, *rest_nodes)

    # Restart stopped node and wait for successful catch up
    lagging_node = start_stopped_node(lagging_node,
                                      looper,
                                      tconf,
                                      tdir,
                                      allPluginsPath,
                                      start=False,
                                      )
    log_re_ask, _ = logsearch(msgs=['requesting .* missing transactions after timeout'])
    old_re_ask_count = len(log_re_ask)

    # Delay ConsProof messages on Alpha
    with delay_rules(rest_nodes[0].nodeIbStasher, cpDelay()):
        # Delay Ctahcupreq messages on Beta
        with delay_rules(rest_nodes[1].nodeIbStasher, cqDelay()):
            looper.add(lagging_node)
            txnPoolNodeSet[-1] = lagging_node
            looper.run(checkNodesConnected(txnPoolNodeSet))

            waitNodeDataEquality(looper, *txnPoolNodeSet, customTimeout=120,
                         exclude_from_check=['check_last_ordered_3pc_backup'])
            assert len(log_re_ask) - old_re_ask_count == 2  # for audit and domain ledgers
