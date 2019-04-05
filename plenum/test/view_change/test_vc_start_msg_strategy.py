import json
from collections import deque

import functools
import pytest

from plenum.common.messages.node_messages import ViewChangeStartMessage, ViewChangeContinueMessage, Prepare, \
    InstanceChange
from plenum.common.timer import QueueTimer
from plenum.common.util import get_utc_epoch
from plenum.server.node import Node
from plenum.server.router import Router
from plenum.server.view_change.node_view_changer import create_view_changer
from plenum.server.view_change.pre_view_change_strategies import VCStartMsgStrategy
from plenum.test.testing_utils import FakeSomething
from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_zmq.zstack import Quota


@pytest.fixture(scope="function")
def fake_node(tconf):
    node = FakeSomething(config=tconf,
                         timer=QueueTimer(),
                         nodeStatusDB=None,
                         master_replica=FakeSomething(inBox=deque(),
                                                      inBoxRouter=Router(),
                                                      logger=FakeSomething(
                                                          info=lambda *args, **kwargs: True
                                                      )),
                         name="Alpha",
                         master_primary_name="Alpha",
                         on_view_change_start=lambda *args, **kwargs: True,
                         start_catchup=lambda *args, **kwargs: True,
                         nodeInBox=deque(),
                         nodeMsgRouter=Router(),
                         metrics=None,
                         process_one_node_message=None,
                         quota_control=FakeSomething(
                             node_quota=Quota(count=100,
                                              size=100)),
                         nodestack=FakeSomething(
                             service=lambda *args, **kwargs: eventually(lambda: True)
                         ))
    node.metrics = functools.partial(Node._createMetricsCollector, node)()
    node.process_one_node_message = functools.partial(Node.process_one_node_message, node)
    return node


@pytest.fixture(scope="function")
def view_changer(fake_node):
    view_changer = create_view_changer(fake_node)
    fake_node.view_changer = view_changer
    fake_node.viewNo = view_changer.view_no
    fake_node.master_replica.node = fake_node
    return view_changer


@pytest.fixture(scope="function")
def pre_vc_strategy(view_changer, fake_node):
    strategy = VCStartMsgStrategy(view_changer, fake_node)
    strategy.view_changer.pre_vc_strategy = strategy
    return strategy


def test_set_is_preparing_on_first_prepare_view_change_call(pre_vc_strategy):
    pre_vc_strategy.is_preparing = False
    pre_vc_strategy.prepare_view_change(1)
    assert pre_vc_strategy.is_preparing


def test_set_req_handlers_on_first_prepare_view_change_call(pre_vc_strategy):
    pre_vc_strategy.is_preparing = False
    pre_vc_strategy.prepare_view_change(1)

    """Check, that req handler is set for ViewChangeStartMessage on node's nodeMsgRouter"""
    assert len(pre_vc_strategy.node.nodeMsgRouter.routes) == 1
    req_handler = pre_vc_strategy.node.nodeMsgRouter.routes.get(ViewChangeStartMessage, None)
    assert req_handler
    assert req_handler.func.__name__ == VCStartMsgStrategy.on_view_change_started.__name__

    """Check, that req handler is set for ViewChangeContinuedMessage on replica's inBoxRouter"""
    assert len(pre_vc_strategy.node.master_replica.inBoxRouter.routes) == 1
    req_handler = pre_vc_strategy.node.master_replica.inBoxRouter.routes.get(ViewChangeContinueMessage, None)
    assert req_handler
    assert req_handler.func.__name__ == VCStartMsgStrategy.on_view_change_continued.__name__


def test_add_vc_start_msg(pre_vc_strategy):
    pre_vc_strategy.is_preparing = False
    pre_vc_strategy.prepare_view_change(1)

    """Check, that ViewChangeStartMessage is added to nodeInBox queue"""
    assert len(pre_vc_strategy.node.nodeInBox) == 1
    m = pre_vc_strategy.node.nodeInBox.popleft()
    assert isinstance(m, MessageBase)
    assert m.frm == pre_vc_strategy.node.name
    assert isinstance(m.msg, ViewChangeStartMessage)


def test_do_not_add_vcs_msg_if_is_preparing_true(pre_vc_strategy):
    pre_vc_strategy.is_preparing = True
    pre_vc_strategy.prepare_view_change(1)
    assert len(pre_vc_strategy.node.nodeInBox) == 0


def test_do_not_set_req_handlers_if_is_preparing_true(pre_vc_strategy):
    pre_vc_strategy.is_preparing = True
    pre_vc_strategy.prepare_view_change(1)
    assert len(pre_vc_strategy.node.nodeMsgRouter.routes) == 0


def test_not_continue_vc_with_proposed_view_no_less_then_current(pre_vc_strategy, looper):
    pre_vc_strategy.view_changer.view_no = 2
    looper.run(pre_vc_strategy.on_view_change_started(pre_vc_strategy.node,
                                                      ViewChangeStartMessage(1),
                                                      "some_node"))
    assert len(pre_vc_strategy.replica.inBox) == 0


def test_add_vc_continued_msg_on_view_change_started(pre_vc_strategy, looper):
    pre_vc_strategy.view_changer.view_no = 1
    looper.run(pre_vc_strategy.on_view_change_started(pre_vc_strategy.node,
                                                      ViewChangeStartMessage(2),
                                                      "some_node"))
    assert len(pre_vc_strategy.node.master_replica.inBox) == 1
    m = pre_vc_strategy.node.master_replica.inBox.popleft()
    assert isinstance(m, ViewChangeContinueMessage)


def test_stash_not_3PC_msgs(pre_vc_strategy, looper):
    node = pre_vc_strategy.node
    pre_vc_strategy.view_changer.view_no = 1
    m1 = InstanceChange(3, 25, frm='Beta', ts_rcv=1)
    m2 = InstanceChange(3, 25, frm='Gamma', ts_rcv=2)
    assert len(pre_vc_strategy.stashedNodeInBox) == 0
    node.nodeInBox.append(m1)
    node.nodeInBox.append(m2)
    looper.run(pre_vc_strategy.on_view_change_started(pre_vc_strategy.node,
                                                      ViewChangeStartMessage(2),
                                                      "some_node"))
    assert len(pre_vc_strategy.stashedNodeInBox) > 0
    """In the same order as from nodeInBox queue"""
    assert pre_vc_strategy.stashedNodeInBox.popleft() == m1
    assert pre_vc_strategy.stashedNodeInBox.popleft() == m2


def test_the_same_order_as_in_NodeInBox_after_vc_continued(pre_vc_strategy):
    replica = pre_vc_strategy.replica
    pre_vc_strategy.view_changer.view_no = 1
    m1 = InstanceChange(3, 25, frm='Beta', ts_rcv=1)
    m2 = InstanceChange(3, 25, frm='Gamma', ts_rcv=2)
    pre_vc_strategy.stashedNodeInBox.append(m1)
    pre_vc_strategy.stashedNodeInBox.append(m2)
    assert len(pre_vc_strategy.node.nodeInBox) == 0
    pre_vc_strategy.on_view_change_continued(replica, ViewChangeContinueMessage(2))
    assert len(pre_vc_strategy.node.nodeInBox) > 0
    assert pre_vc_strategy.node.nodeInBox.popleft() == m1
    assert pre_vc_strategy.node.nodeInBox.popleft() == m2


def test_is_preparing_to_False_after_vc_continue(pre_vc_strategy):
    replica = pre_vc_strategy.replica
    pre_vc_strategy.is_preparing = True
    pre_vc_strategy.on_view_change_continued(replica, ViewChangeContinueMessage(2))
    assert pre_vc_strategy.is_preparing == False


def test_get_msgs_from_rxMsgs_queue(create_node_and_not_start, looper):
    node = create_node_and_not_start
    node.view_changer = create_view_changer(node)
    node.view_changer.pre_vc_strategy = VCStartMsgStrategy(view_changer, node)
    node.view_changer.view_no = 0
    """pre_view_change stage"""
    node.view_changer.start_view_change(1)
    assert node.view_changer.view_no == 0
    prepare = Prepare(
        0,
        0,
        1,
        get_utc_epoch(),
        'f99937241d4c891c08e92a3cc25966607315ca66b51827b170d492962d58a9be',
        'CZecK1m7VYjSNCC7pGHj938DSW2tfbqoJp1bMJEtFqvG',
        '7WrAMboPTcMaQCU1raoj28vnhu2bPMMd2Lr9tEcsXeCJ')
    inst_change = InstanceChange(1, 25)
    m = node.nodeInBox.popleft()
    assert isinstance(m.msg, ViewChangeStartMessage)
    node.nodestack.addRemote('someNode', genHa(), b'1DYuELN<SHbv1?NJ=][4De%^Hge887B0I!s<YGdD', 'pubkey')
    node.nodestack.rxMsgs.append((json.dumps(prepare._asdict()), 'pubkey', 1))
    node.nodestack.rxMsgs.append((json.dumps(inst_change._asdict()), 'pubkey', 2))
    node.msgHasAcceptableViewNo = lambda *args, **kwargs: True
    """While processing ViewChangeStartMessage from nodeInBox queue, should be:
    - move msgs from rxMsgs queue to nodeInBox queue
    - process all 3PC msgs (for Prepare msg it should be moved to inBox queue of master_replica)
    - add ViewChangeContinue msg into master_replica's inBox queue
    - all not 3PC msgs will be stashed in strategy queue"""
    looper.run(node.process_one_node_message(m))
    m = node.master_replica.inBox.popleft()
    assert isinstance(m.msg, Prepare)
    m = node.master_replica.inBox.popleft()
    assert isinstance(m, ViewChangeContinueMessage)
    m = node.view_changer.pre_vc_strategy.stashedNodeInBox.popleft()
    assert isinstance(m.msg, InstanceChange)
