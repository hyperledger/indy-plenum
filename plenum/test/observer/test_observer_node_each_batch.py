import pytest

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.keygen_utils import initNodeKeysForBothStacks
from plenum.common.util import randomString
from plenum.server.observer.observer_node import NodeObserver
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import checkNodeDataForEquality
from plenum.test.pool_transactions.helper import new_node
from stp_core.network.port_dispenser import genHa


@pytest.fixture(scope="module")
def fake_node(txnPoolNodeSet,
              testNodeClass,
              tdir, tconf,
              allPluginsPath):
    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)
    newNodeName = "NewTestNode"
    sigseed = randomString(32).encode()
    config_helper = PNodeConfigHelper(newNodeName, tconf, chroot=tdir)
    _, verkey, bls_key = initNodeKeysForBothStacks(newNodeName, config_helper.keys_dir,
                                                   sigseed, override=True)
    node = new_node(node_name=newNodeName,
                    tdir=tdir,
                    node_ha=(nodeIp, nodePort),
                    client_ha=(clientIp, clientPort),
                    tconf=tconf,
                    plugin_path=allPluginsPath,
                    nodeClass=testNodeClass)
    yield node
    node.stop()


@pytest.fixture(scope="module")
def observed_data_msg(looper,
                      txnPoolNodeSet,
                      sdk_pool_handle, sdk_wallet_client):
    txnPoolNodeSet[0]._observable.add_observer("observer1", ObserverSyncPolicyType.EACH_BATCH)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              1)

    return txnPoolNodeSet[0]._observable.get_output()[0]

@pytest.fixture(scope="module")
def fake_node_observer(fake_node):
    return NodeObserver(fake_node)

def test_apply_data(fake_node,
                    fake_node_observer,
                    txnPoolNodeSet,
                    observed_data_msg):
    fake_node_observer.apply_data(observed_data_msg)
    checkNodeDataForEquality(fake_node,
                             *txnPoolNodeSet,
                             exclude_from_check=['check_last_ordered_3pc'])
