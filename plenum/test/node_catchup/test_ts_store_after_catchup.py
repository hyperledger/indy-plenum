from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.txn_util import get_req_id, get_from, get_txn_time, get_payload_data
from plenum.test.buy_handler import BuyHandler
from plenum.test.constants import GET_BUY
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node


def test_fill_ts_store_after_catchup(txnPoolNodeSet,
                                     looper,
                                     sdk_pool_handle,
                                     sdk_wallet_steward,
                                     tconf,
                                     tdir,
                                     allPluginsPath
                                     ):
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)
    node_to_disconnect = txnPoolNodeSet[-1]

    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_disconnect)
    looper.removeProdable(name=node_to_disconnect.name)
    sdk_replies = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                            sdk_pool_handle, sdk_wallet_steward, 2)

    node_to_disconnect = start_stopped_node(node_to_disconnect, looper, tconf,
                                            tdir, allPluginsPath)
    txnPoolNodeSet[-1] = node_to_disconnect
    looper.run(checkNodesConnected(txnPoolNodeSet))

    waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet,
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    req_handler = node_to_disconnect.read_manager.request_handlers[GET_BUY]
    for reply in sdk_replies:
        key = BuyHandler.prepare_buy_key(get_from(reply[1]['result']),
                                           get_req_id(reply[1]['result']))
        root_hash = req_handler.database_manager.ts_store.get_equal_or_prev(get_txn_time(reply[1]['result']))
        assert root_hash
        from_state = req_handler.state.get_for_root_hash(root_hash=root_hash,
                                                         key=key)
        assert domain_state_serializer.deserialize(from_state)['amount'] == \
               get_payload_data(reply[1]['result'])['amount']
