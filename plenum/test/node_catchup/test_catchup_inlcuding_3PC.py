import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import check_if_all_equal_in_list
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import check_last_3pc_master, \
    waitNodeDataEquality
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet
from plenum.test.test_client import TestClient
from stp_core.loop.eventually import eventually


TestRunningTimeLimitSec = 125


def chk_if_equal_txn_to_3pc(nodes, count=None):
    txn_to_tpc = []
    for node in nodes:
        txn_to_tpc.append(node.txn_seq_range_to_3phase_key[DOMAIN_LEDGER_ID])
    assert check_if_all_equal_in_list(txn_to_tpc)
    if count is not None:
        assert len(txn_to_tpc[0]) == count


@pytest.fixture("module")
def tconf(tconf, request):
    old_size = tconf.ProcessedBatchMapsToKeep
    tconf.ProcessedBatchMapsToKeep = 5

    def reset():
        tconf.ProcessedBatchMapsToKeep = old_size

    request.addfinalizer(reset)
    return tconf


@pytest.fixture("module")
def pre_check(tconf, looper, txnPoolNodeSet, tdirWithClientPoolTxns,
              poolTxnStewardData):
    # TODO: Maybe this needs to be extracted in another fixture
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithClientPoolTxns,
                                              clientClass=TestClient)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    for i in range(tconf.ProcessedBatchMapsToKeep - 1):
        sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 1)

    # All node maintain the same map from txn range to 3PC
    looper.run(eventually(chk_if_equal_txn_to_3pc, txnPoolNodeSet))
    for i in range(3):
        sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 1)

    # All node maintain the same map from txn range to 3PC and its equal to
    # `tconf.ProcessedBatchMapsToKeep` even after sending more batches than
    # `tconf.ProcessedBatchMapsToKeep`, which shows the garbage cleaning in
    # action
    looper.run(eventually(chk_if_equal_txn_to_3pc, txnPoolNodeSet,
                          tconf.ProcessedBatchMapsToKeep))


def test_nodes_maintain_master_txn_3PC_map(looper, txnPoolNodeSet, pre_check,
                                           nodeCreatedAfterSomeTxns):
    _, new_node, client, wallet, _, _ = nodeCreatedAfterSomeTxns
    txnPoolNodeSet.append(new_node)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])
    # Check the new node has set same `last_3pc_ordered` for master as others
    check_last_3pc_master(new_node, txnPoolNodeSet[:4])
    chk_if_equal_txn_to_3pc(txnPoolNodeSet[:4])
    # Requests still processed
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 2)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])
