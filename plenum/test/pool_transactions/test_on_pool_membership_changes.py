import pytest

from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.test_node import TestNode

from plenum.common.txn_util import get_payload_data

from plenum.common.constants import TARGET_NYM, DATA, SERVICES, VALIDATOR, TXN_PAYLOAD
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(scope='function')
def test_node(tdirWithPoolTxns,
              tdirWithDomainTxns,
              poolTxnNodeNames,
              tdirWithNodeKeepInited,
              tdir,
              tconf,
              allPluginsPath):
    node_name = poolTxnNodeNames[0]
    config_helper = PNodeConfigHelper(node_name, tconf, chroot=tdir)
    node = TestNode(
        node_name,
        config_helper=config_helper,
        config=tconf,
        pluginPaths=allPluginsPath)
    node.view_changer = FakeSomething(view_change_in_progress=False,
                                      view_no=0)
    yield node
    node.onStopping()


def test_on_pool_membership_changes(test_node, pool_node_txns):
    def get_all_txn():
        assert False, "ledger.getAllTxn() shouldn't be called in onPoolMembershipChange()"

    pool_manager = test_node.poolManager
    pool_manager.ledger.getAllTxn = get_all_txn

    txn = pool_node_txns[0]
    node_nym = get_payload_data(txn)[TARGET_NYM]

    txn[TXN_PAYLOAD][DATA][DATA][SERVICES] = []
    assert node_nym in pool_manager._ordered_node_services
    pool_manager.onPoolMembershipChange(txn)
    assert pool_manager._ordered_node_services[node_nym] == []

    txn[TXN_PAYLOAD][DATA][DATA][SERVICES] = [VALIDATOR]
    pool_manager.onPoolMembershipChange(txn)
    assert pool_manager._ordered_node_services[node_nym] == [VALIDATOR]

    txn[TXN_PAYLOAD][DATA][DATA][SERVICES] = []
    pool_manager.onPoolMembershipChange(pool_node_txns[0])
    assert pool_manager._ordered_node_services[node_nym] == []
