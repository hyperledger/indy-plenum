from plenum.common.constants import DOMAIN_LEDGER_ID, NYM
from plenum.common.txn_util import get_txn_time
from plenum.common.util import get_utc_epoch
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import get_master_primary_node, checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node


def test_get_last_ordered_timestamp_after_catchup(looper,
                                                  txnPoolNodeSet,
                                                  sdk_pool_handle,
                                                  sdk_wallet_steward,
                                                  tconf,
                                                  tdir,
                                                  allPluginsPath):
    node_to_disconnect = txnPoolNodeSet[-1]
    reply_before = sdk_send_random_and_check(looper,
                                             txnPoolNodeSet,
                                             sdk_pool_handle,
                                             sdk_wallet_steward,
                                             1)[0][1]
    looper.runFor(2)
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_disconnect)
    looper.removeProdable(name=node_to_disconnect.name)
    reply = sdk_send_random_and_check(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_steward,
                                      1)[0][1]

    node_to_disconnect = start_stopped_node(node_to_disconnect, looper, tconf,
                                            tdir, allPluginsPath)
    txnPoolNodeSet[-1] = node_to_disconnect
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet[:-1],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    ts_from_state = node_to_disconnect.master_replica._get_last_timestamp_from_state(DOMAIN_LEDGER_ID)
    assert ts_from_state == get_txn_time(reply['result'])
    assert ts_from_state != get_txn_time(reply_before['result'])


def test_choose_ts_from_state(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward):
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward,
                              1)
    primary_node = get_master_primary_node(txnPoolNodeSet)
    excpected_ts = get_utc_epoch() + 30
    req_handler = primary_node.write_manager.request_handlers[NYM][0]
    req_handler.database_manager.ts_store.set(excpected_ts,
                                              req_handler.state.headHash)
    primary_node.master_replica._ordering_service.last_accepted_pre_prepare_time = None
    reply = sdk_send_random_and_check(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_steward,
                                      1)[0][1]
    assert abs(excpected_ts - int(get_txn_time(reply['result']))) < 3
