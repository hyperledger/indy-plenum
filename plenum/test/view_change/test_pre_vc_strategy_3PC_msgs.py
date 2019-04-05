from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Prepare, Commit, PrePrepare, ViewChangeStartMessage, \
    ViewChangeContinueMessage
from plenum.common.util import get_utc_epoch
from plenum.server.view_change.pre_view_change_strategies import VCStartMsgStrategy


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
        True,
        frm='some_node',
        ts_rcv=get_utc_epoch() + 1
    )
    prepare = Prepare(
        0,
        0,
        1,
        get_utc_epoch(),
        'f99937241d4c891c08e92a3cc25966607315ca66b51827b170d492962d58a9be',
        'CZecK1m7VYjSNCC7pGHj938DSW2tfbqoJp1bMJEtFqvG',
        '7WrAMboPTcMaQCU1raoj28vnhu2bPMMd2Lr9tEcsXeCJ',
        frm='some_node',
        ts_rcv=get_utc_epoch() + 1
    )
    commit = Commit(
        0,
        0,
        1,
        frm='some_node',
        ts_rcv=get_utc_epoch() + 1
    )
    node.view_changer = node.newViewChanger()
    node.view_changer.pre_vc_strategy = VCStartMsgStrategy(node.view_changer, node)
    node.view_changer.view_no = 0
    node.master_replica.primaryName = 'Alpha'
    """Initiate view_change procedure"""
    node.view_changer.start_view_change(1)
    assert len(node.nodeInBox) == 1
    m = node.nodeInBox[0]
    assert isinstance(m.msg, ViewChangeStartMessage)
    """
    Imitate that nodestack.service was called and
    those of next messages are put into nodeInBox queue
    """
    node.nodeInBox.append(preprepare)
    node.nodeInBox.append(prepare)
    node.nodeInBox.append(commit)
    assert len(node.master_replica.inBox) == 0
    """Imitate looper's work"""
    looper.run(node.serviceReplicas(None))
    """
    Next looper's task must be processNodeInBox and
    all of 3PC messages must be moved to replica's inBox queue
    """
    looper.run(node.processNodeInBox())
    """3 3PC msgs and 1 is ViewChangeContinuedMessage"""
    assert len(node.master_replica.inBox) == 4
    """Check the order of msgs"""
    m = node.master_replica.inBox.popleft()
    assert isinstance(m.msg, PrePrepare)
    m = node.master_replica.inBox.popleft()
    assert isinstance(m.msg, Prepare)
    m = node.master_replica.inBox.popleft()
    assert isinstance(m.msg, Commit)
    m = node.master_replica.inBox.popleft()
    assert isinstance(m, ViewChangeContinueMessage)
