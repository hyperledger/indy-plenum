from plenum.common.util import hexToFriendly

from plenum.test.pool_transactions.helper import sdk_send_update_node
from plenum.test.test_node import TestNode, checkNodesConnected
from stp_core.network.port_dispenser import genHa
from plenum.common.config_helper import PNodeConfigHelper


def testChangeNodeHaBack(looper, txnPoolNodeSet,
                         sdk_pool_handle,
                         sdk_node_theta_added,
                         tconf, tdir):
    """
    The case:
        The Node HA is updated with some HA (let's name it 'correct' HA).
        Then the Steward makes a mistake and sends the NODE txn with other HA
        ('wrong' HA). The Steward replaces back 'wrong' HA by 'correct' HA sending
        yet another one NODE txn.
    """
    new_steward_wallet, new_node = sdk_node_theta_added
    client_ha = new_node.cliNodeReg['ThetaC']  # use the same client HA
    # do all exercises without the Node
    new_node.stop()
    looper.removeProdable(name=new_node.name)

    # step 1: set 'correct' HA
    correct_node_ha = genHa(1)

    node_dest = hexToFriendly(new_node.nodestack.verhex)
    sdk_send_update_node(looper, new_steward_wallet, sdk_pool_handle,
                         node_dest, new_node.name,
                         correct_node_ha.host, correct_node_ha.port,
                         client_ha.host, client_ha.port)

    # step 2: set 'wrong' HA
    wrong_node_ha = genHa(1)
    sdk_send_update_node(looper, new_steward_wallet, sdk_pool_handle,
                         node_dest, new_node.name,
                         wrong_node_ha.host, wrong_node_ha.port,
                         client_ha.host, client_ha.port)

    # step 3: set 'correct' HA back
    sdk_send_update_node(looper, new_steward_wallet, sdk_pool_handle,
                         node_dest, new_node.name,
                         correct_node_ha.host, correct_node_ha.port,
                         client_ha.host, client_ha.port)

    # In order to save the time the pool connection is not maintaining
    # during the steps, only the final result is checked.
    config_helper = PNodeConfigHelper(new_node.name, tconf, chroot=tdir)
    restartedNode = TestNode(new_node.name,
                             config_helper=config_helper,
                             config=tconf, ha=correct_node_ha, cliha=client_ha)
    looper.add(restartedNode)
    txnPoolNodeSet[-1] = restartedNode

    looper.run(checkNodesConnected(txnPoolNodeSet))
    # check Theta HA
    for n in txnPoolNodeSet:
        assert n.nodeReg['Theta'] == correct_node_ha
