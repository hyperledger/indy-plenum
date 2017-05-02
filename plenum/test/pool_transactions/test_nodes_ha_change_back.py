from plenum.test.pool_transactions.helper import changeNodeHa
from plenum.test.test_node import TestNode, checkNodesConnected
from stp_core.network.port_dispenser import genHa


def testChangeNodeHaBack(looper, txnPoolNodeSet, tdirWithPoolTxns,
                         tconf, steward1, stewardWallet, nodeThetaAdded):
    """
    Case (https://evernym.atlassian.net/browse/SOV-908):
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
    changeNodeHa(looper, steward, stewardWallet, theta,
                 nodeHa=correctNodeHa, clientHa=clientHa)

    # step 2: set 'wrong' HA
    wrongNodeHa = genHa(1)
    changeNodeHa(looper, steward, stewardWallet, theta,
                 nodeHa=wrongNodeHa, clientHa=clientHa)

    # step 3: set 'correct' HA back
    changeNodeHa(looper, steward, stewardWallet, theta,
                 nodeHa=correctNodeHa, clientHa=clientHa)

    # In order to save the time the pool connection is not maintaining
    # during the steps, only the final result is checked.
    restartedNode = TestNode(theta.name, basedirpath=tdirWithPoolTxns,
                             config=tconf, ha=correctNodeHa, cliha=clientHa)
    looper.add(restartedNode)
    txnPoolNodeSet[-1] = restartedNode

    looper.run(checkNodesConnected(txnPoolNodeSet))
    # check Theta HA
    for n in txnPoolNodeSet:
        assert n.nodeReg['Theta'] == correctNodeHa