from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.internal_messages import NodeNeedViewChange
from plenum.common.messages.node_messages import Prepare, Commit, PrePrepare
from plenum.common.util import get_utc_epoch


def test_accept_all_3PC_msgs(create_node_and_not_start, looper):
    node = create_node_and_not_start
    preprepare = PrePrepare(
        0,
        0,
        1,
        get_utc_epoch(),
        ['f99937241d4c891c08e92a3cc25966607315ca66b51827b170d492962d58a9be'],
        '[]',
        'f99937241d4c891c08e92a3cc25966607315ca66b51827b170d492962d58a9be',
        DOMAIN_LEDGER_ID,
        'CZecK1m7VYjSNCC7pGHj938DSW2tfbqoJp1bMJEtFqvG',
        '7WrAMboPTcMaQCU1raoj28vnhu2bPMMd2Lr9tEcsXeCJ',
        0,
        True
    )
    prepare = Prepare(
        0,
        0,
        1,
        get_utc_epoch(),
        'f99937241d4c891c08e92a3cc25966607315ca66b51827b170d492962d58a9be',
        'CZecK1m7VYjSNCC7pGHj938DSW2tfbqoJp1bMJEtFqvG',
        '7WrAMboPTcMaQCU1raoj28vnhu2bPMMd2Lr9tEcsXeCJ')
    commit = Commit(
        0,
        0,
        1
    )
    node.master_replica._consensus_data.view_no = 0
    node.master_replica.primaryName = 'Alpha'
    """Initiate view_change procedure"""
    node.master_replica.internal_bus.send(NodeNeedViewChange(1))
    # We don't use a view_change_strategy anymore
    assert len(node.nodeInBox) == 0
    """
    Imitate that nodestack.service was called and 
    those of next messages are put into nodeInBox queue
    """
    node.nodeInBox.append((preprepare, 'some_node'))
    node.nodeInBox.append((prepare, 'some_node'))
    node.nodeInBox.append((commit, 'some_node'))
    assert len(node.master_replica.inBox) == 0
    """Imitate looper's work"""
    looper.run(node.serviceReplicas(None))
    """
    Next looper's task must be processNodeInBox and 
    all of 3PC messages must be moved to replica's inBox queue
    """
    looper.run(node.processNodeInBox())
    """3 3PC msgs"""
    assert len(node.master_replica.inBox) == 3
    """Check the order of msgs"""
    m = node.master_replica.inBox.popleft()
    assert isinstance(m[0], PrePrepare)
    m = node.master_replica.inBox.popleft()
    assert isinstance(m[0], Prepare)
    m = node.master_replica.inBox.popleft()
    assert isinstance(m[0], Commit)
