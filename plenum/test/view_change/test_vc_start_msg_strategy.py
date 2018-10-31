from collections import deque

import pytest

from plenum.common.messages.node_messages import ViewChangeStartMessage, ViewChangeContinueMessage
from plenum.server.router import Router
from plenum.server.view_change.view_changer import ViewChanger
from plenum.test.testing_utils import FakeSomething


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
                         nodeMsgRouter=Router())
    view_changer = ViewChanger(node)
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



