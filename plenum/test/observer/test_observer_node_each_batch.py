import pytest

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.keygen_utils import initNodeKeysForBothStacks
from plenum.common.util import randomString
from plenum.server.observer.observer_node import NodeObserver
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import checkNodeDataForEquality, checkNodeDataForInequality
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
    _, verkey, bls_key, _ = initNodeKeysForBothStacks(newNodeName,
                                                      config_helper.keys_dir,
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
def observed_data_msgs(looper,
                       txnPoolNodeSet,
                       sdk_pool_handle, sdk_wallet_client):
    txnPoolNodeSet[0]._observable.add_observer("observer1",
                                               ObserverSyncPolicyType.EACH_BATCH)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              10)

    msgs = []
    while True:
        msg = txnPoolNodeSet[0]._observable.get_output()
        if msg is None:
            break
        msgs.append(msg[0])
    return msgs


@pytest.fixture(scope="module")
def fake_node_observer(fake_node):
    return NodeObserver(fake_node)


def test_apply_data(fake_node,
                    fake_node_observer,
                    txnPoolNodeSet,
                    observed_data_msgs,
                    looper):
    '''
    - Create a Node (do not add it to the pool) with Observer.
    - Send txns and get ObservedData msgs from Validators.looper
    - Apply the ObservedData masgs by the Observer and make sure that it becomes synced with the pool.
    '''
    looper.add(fake_node)
    # check that Observer is not synced with the pool
    checkNodeDataForInequality(fake_node,
                               *txnPoolNodeSet,
                               exclude_from_check=['check_last_ordered_3pc',
                                                   'check_seqno_db'])

    # emulate sending of ObserverData from each Node
    for observed_data_msg in observed_data_msgs:
        for node in txnPoolNodeSet:
            fake_node_observer.apply_data(observed_data_msg, node.name)

    # check that Observer is synced with the pool
    checkNodeDataForEquality(fake_node,
                             *txnPoolNodeSet,
                             exclude_from_check=['check_last_ordered_3pc',
                                                 'check_primaries',
                                                 'check_last_ordered_3pc_backup'])
