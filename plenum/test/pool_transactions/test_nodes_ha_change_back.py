from plenum.common.constants import ALIAS, NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT
from plenum.test.pool_transactions.helper import updateNodeData
from plenum.test.test_node import TestNode, checkNodesConnected
from stp_core.network.port_dispenser import genHa
from plenum.common.config_helper import PNodeConfigHelper

def testChangeNodeHaBack(looper, txnPoolNodeSet, tdir, tconf,
                         steward1, stewardWallet, nodeThetaAdded):
    """
    The case:
        The Node HA is updated with some HA (let's name it 'correct' HA).
        Then the Steward makes a mistake and sends the NODE txn with other HA
        ('wrong' HA). The Steward replaces back 'wrong' HA by 'correct' HA sending
        yet another one NODE txn.
    """

    steward, stewardWallet, theta = nodeThetaAdded
    clientHa = theta.cliNodeReg['ThetaC']  # use the same client HA
    # do all exercises without the Node
    theta.stop()
    looper.removeProdable(name=theta.name)

    # step 1: set 'correct' HA
    correctNodeHa = genHa(1)
    op = {
        ALIAS: theta.name,
        NODE_IP: correctNodeHa.host,
        NODE_PORT: correctNodeHa.port,
        CLIENT_IP: clientHa.host,
        CLIENT_PORT: clientHa.port,
    }
    updateNodeData(looper, steward, stewardWallet, theta,
                   op)

    # step 2: set 'wrong' HA
    wrongNodeHa = genHa(1)
    op.update({NODE_IP: wrongNodeHa.host, NODE_PORT: wrongNodeHa.port})
    updateNodeData(looper, steward, stewardWallet, theta,
                   op)

    # step 3: set 'correct' HA back
    op.update({NODE_IP: correctNodeHa.host, NODE_PORT: correctNodeHa.port})
    updateNodeData(looper, steward, stewardWallet, theta,
                   op)

    # In order to save the time the pool connection is not maintaining
    # during the steps, only the final result is checked.
    config_helper = PNodeConfigHelper(theta.name, tconf, chroot=tdir)
    restartedNode = TestNode(theta.name,
                             config_helper=config_helper,
                             config=tconf, ha=correctNodeHa, cliha=clientHa)
    looper.add(restartedNode)
    txnPoolNodeSet[-1] = restartedNode

    looper.run(checkNodesConnected(txnPoolNodeSet))
    # check Theta HA
    for n in txnPoolNodeSet:
        assert n.nodeReg['Theta'] == correctNodeHa
