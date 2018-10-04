import pytest
from stp_core.loop.eventually import eventually

from plenum.common.metrics_collector import MetricsName

from plenum.test.helper import sdk_send_random_and_check

from plenum.common.txn_util import get_type, get_payload_data

from plenum.common.constants import TARGET_NYM, NODE, CLIENT_STACK_SUFFIX, DATA, ALIAS, SERVICES
from plenum.test.pool_transactions.helper import demote_node

nodeCount = 7
nodes_wth_bls = 0


@pytest.fixture()
def pool_node_txns(poolTxnData):
    node_txns = []
    for txn in poolTxnData["txns"]:
        if get_type(txn) == NODE:
            node_txns.append(txn)
    return node_txns


def test_twice_demoted_node_dont_write_txns(txnPoolNodeSet,
                                            looper, sdk_wallet_stewards, sdk_pool_handle):
    request_count = 5
    demoted_node = txnPoolNodeSet[2]
    alive_pool = list(txnPoolNodeSet)
    alive_pool.remove(demoted_node)

    def get_node_prods_count(node):
        return node.metrics._accumulators[MetricsName.NODE_PROD_TIME].count

    def is_prods_run(node, old, diff):
        new = get_node_prods_count(node)
        assert old + diff <= new

    demote_node(looper, sdk_wallet_stewards[2], sdk_pool_handle, demoted_node)
    demote_node(looper, sdk_wallet_stewards[2], sdk_pool_handle, demoted_node)

    demoted_nym = None
    for _, txn in txnPoolNodeSet[0].poolManager.ledger.getAllTxn():
        txn_data = get_payload_data(txn)
        if txn_data[DATA][ALIAS] == demoted_node.name:
            demoted_nym = txn_data[TARGET_NYM]
            break
    assert demoted_nym
    # Every node demote `demoted_node`
    assert all(node.poolManager.reqHandler.getNodeData(demoted_nym)[SERVICES] == []
               for node in alive_pool)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_stewards[0], request_count)

    old = get_node_prods_count(txnPoolNodeSet[0])

    # Let primary node make 2 prod runs so we make sure that
    # node did not appear in network reconnection
    looper.run(eventually(is_prods_run, txnPoolNodeSet[0], old, 2))

    assert txnPoolNodeSet[0].domainLedger.size - request_count == \
           demoted_node.domainLedger.size


def test_get_nym_by_name(txnPoolNodeSet, pool_node_txns):
    check_get_nym_by_name(txnPoolNodeSet, pool_node_txns)


def test_get_nym_by_name_not_in_registry(txnPoolNodeSet, pool_node_txns):
    nodes_to_remove = [txnPoolNodeSet[4].name, txnPoolNodeSet[5].name]
    for node in txnPoolNodeSet:
        for node_to_remove in nodes_to_remove:
            del node.nodeReg[node_to_remove]
            del node.cliNodeReg[node_to_remove + CLIENT_STACK_SUFFIX]
    check_get_nym_by_name(txnPoolNodeSet, pool_node_txns)


def test_get_nym_by_name_demoted(txnPoolNodeSet, pool_node_txns,
                                 looper, sdk_wallet_stewards, sdk_pool_handle):
    demote_node(looper, sdk_wallet_stewards[0], sdk_pool_handle,
                txnPoolNodeSet[0])
    check_get_nym_by_name(txnPoolNodeSet, pool_node_txns)


def check_get_nym_by_name(txnPoolNodeSet, pool_node_txns):
    for i in range(nodeCount):
        node = txnPoolNodeSet[i]
        pool_manager = node.poolManager
        node_name = node.name

        node_nym = pool_manager.get_nym_by_name(node_name)
        expected_data = get_payload_data(pool_node_txns[i])[TARGET_NYM]

        assert node_nym
        assert node_nym == expected_data
