import pytest

from plenum.test.conftest import getValueFromModule
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.primary_selection.helper import check_newly_added_nodes, \
    getPrimaryNodesIdxs
from plenum.test.pool_transactions.conftest import sdk_node_theta_added


@pytest.fixture(scope='module')
def sdk_one_node_added(looper, txnPoolNodeSet, sdk_node_theta_added):
    # New node knows primary same primary as others and has rank greater
    # than others
    _, new_node = sdk_node_theta_added
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    check_newly_added_nodes(looper, txnPoolNodeSet, [new_node])
    return new_node


@pytest.fixture(scope="module")
def txnPoolMasterNodes(txnPoolNodeSet):
    primariesIdxs = getPrimaryNodesIdxs(txnPoolNodeSet)
    return txnPoolNodeSet[primariesIdxs[0]], txnPoolNodeSet[primariesIdxs[1]]


@pytest.fixture(scope="module")
def checkpoint_size(tconf, request):
    oldChkFreq = tconf.CHK_FREQ
    oldLogSize = tconf.LOG_SIZE
    oldMax3PCBatchSize = tconf.Max3PCBatchSize

    tconf.Max3PCBatchSize = 3
    tconf.CHK_FREQ = getValueFromModule(request, "CHK_FREQ", 2)
    tconf.LOG_SIZE = 2 * tconf.CHK_FREQ

    def reset():
        tconf.CHK_FREQ = oldChkFreq
        tconf.LOG_SIZE = oldLogSize
        tconf.Max3PCBatchSize = oldMax3PCBatchSize

    request.addfinalizer(reset)

    return tconf.CHK_FREQ * tconf.Max3PCBatchSize
