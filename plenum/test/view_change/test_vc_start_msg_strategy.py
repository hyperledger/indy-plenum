import json
from collections import deque

import functools
import pytest

from plenum.common.constants import PreVCStrategies
from plenum.common.messages.node_messages import ViewChangeStartMessage, ViewChangeContinueMessage, Prepare, \
    InstanceChange
from plenum.common.util import get_utc_epoch
from plenum.server.node import Node
from plenum.server.router import Router
from plenum.server.view_change.view_changer import ViewChanger
from plenum.test.testing_utils import FakeSomething
from stp_core.network.port_dispenser import genHa


@pytest.fixture(scope="module")
def tconf(tconf):
    tconf.PRE_VC_STRATEGY = PreVCStrategies.VC_START_MSG_STRATEGY
    yield tconf


@pytest.fixture(scope="function")
def view_changer(tconf):
    node = FakeSomething(config=tconf,
                         master_replica=FakeSomething(inBox=deque(),
                                                      inBoxRouter=Router()),
                         name="Alpha",
                         master_primary_name="Alpha",
                         on_view_change_start=lambda *args, **kwargs: True,
                         start_catchup=lambda *args, **kwargs: True,
                         nodeInBox=deque(),
                         nodeMsgRouter=Router(),
                         metrics=None,
                         process_one_node_message=None)
    node.metrics = functools.partial(Node._createMetricsCollector, node)()
    node.process_one_node_message = functools.partial(Node.process_one_node_message, node)
    view_changer = ViewChanger(node)
    node.view_changer = view_changer
    return view_changer


def test_not_call_strategy_if_propagate_primary_is_true(view_changer):
    view_changer.view_no = 0
    view_changer.propagate_primary = True
    view_changer.startViewChange(1)
    assert view_changer.view_no == 1


def test_not_call_strategy_if_pre_vc_strategy_none(view_changer):
    view_changer.view_no = 0
    view_changer.pre_vc_strategy = None
    view_changer.startViewChange(1)
    assert view_changer.view_no == 1


def test_not_call_strategy_if_continue_vc_true(view_changer):
    view_changer.view_no = 0
    view_changer.startViewChange(1, continue_vc=True)
    assert view_changer.view_no == 1


def test_add_vcstart_msg_and_set_handlers(view_changer):
    view_changer.view_no = 0
    view_changer.startViewChange(1)
    assert view_changer.view_no == 0
    m = view_changer.node.nodeInBox.pop()
    assert isinstance(m[0], ViewChangeStartMessage)
    nodes_handler = view_changer.node.nodeMsgRouter.getFunc(ViewChangeStartMessage(1))
    assert nodes_handler
    replicas_handler = view_changer.node.master_replica.inBoxRouter.getFunc(ViewChangeContinueMessage(1))
    assert replicas_handler


def test_get_msgs_from_rxMsgs_queue(create_node_and_not_start, looper):
    node = create_node_and_not_start
    node.view_changer = ViewChanger(node)
    node.view_changer.view_no = 0
    """pre_view_change stage"""
    node.view_changer.startViewChange(1)
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
    assert isinstance(m[0], ViewChangeStartMessage)
    node.nodestack.addRemote('someNode', genHa(), b'1DYuELN<SHbv1?NJ=][4De%^Hge887B0I!s<YGdD', 'pubkey')
    node.nodestack.rxMsgs.append((json.dumps(prepare._asdict()), 'pubkey'))
    node.nodestack.rxMsgs.append((json.dumps(inst_change._asdict()), 'pubkey'))
    node.msgHasAcceptableViewNo = lambda *args, **kwargs: True
    """While processing ViewChangeStartMessage from nodeInBox queue, should be: 
    - move msgs from rxMsgs queue to nodeInBox queue
    - process all 3PC msgs (for Prepare msg it should be moved to inBox queue of master_replica)
    - add ViewChangeContinue msg into master_replica's inBox queue
    - all not 3PC msgs will be stashed in strategy queue"""
    looper.run(node.process_one_node_message(m))
    m = node.master_replica.inBox.popleft()
    assert isinstance(m[0], Prepare)
    m = node.master_replica.inBox.popleft()
    assert isinstance(m, ViewChangeContinueMessage)
    m = node.view_changer.pre_vc_strategy.stashedNodeInBox.popleft()
    assert isinstance(m[0], InstanceChange)


