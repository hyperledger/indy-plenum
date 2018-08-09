import functools
import pytest

from plenum.common.messages.node_messages import ViewChangeDone
from plenum.server.node import Node
from plenum.server.view_change.view_changer import ViewChanger
from plenum.test.testing_utils import FakeSomething


def check_CurrentState(self, message, rid):
    assert message.viewNo == self.view_changer.last_completed_view_no
    if message.primary:
        for msg in message.primary:
            assert msg.viewNo == self.view_changer.last_completed_view_no


@pytest.fixture(scope="function")
def node_with_nodestack(fake_node):
    nodestack = FakeSomething(name='fake state',
                              connecteds=set(fake_node.allNodeNames),
                              getRemote=lambda name: FakeSomething(uid=name))
    fake_node.nodestack = nodestack
    fake_node.view_changer.last_completed_view_no = fake_node.viewNo
    fake_node.send = functools.partial(check_CurrentState, fake_node)
    fake_node.send_current_state_to_lagging_node = functools.partial(Node.send_current_state_to_lagging_node, fake_node)
    return fake_node


def test_with_last_completed_view_no_view_change_not_in_progress(node_with_nodestack):
    """
    Check that viewNo in CurrentState message is not propagated but last completed
    """
    node_with_nodestack.send_current_state_to_lagging_node('Node2')


def test_with_last_completed_view_no_view_change_in_progress(node_with_nodestack):
    """
    Check that viewNo in CurrentState message is not propagated but last completed
    """
    node_with_nodestack.view_changer.view_change_in_progress = True
    node_with_nodestack.view_changer.view_no = node_with_nodestack.view_changer.view_no + 1
    node_with_nodestack.send_current_state_to_lagging_node('Node2')


def test_view_change_done_in_CS_with_valid_view_no(node_with_nodestack):
    """
    Check, that viewNos in ViewChangeDone messages from CurrentState is not propagated but last completed
    """
    node_with_nodestack.view_changer._accepted_view_change_done_message = ('someNode',
                                                                           node_with_nodestack.ledger_summary)
    node_with_nodestack.send_current_state_to_lagging_node('Node2')


def test_vcdone_in_CS_with_valid_view_no_vc_in_progress(node_with_nodestack):
    """
    Check, that viewNos in ViewChangeDone messages from CurrentState is not propagated but last completed
    """
    node_with_nodestack.view_changer.view_change_in_progress = True
    node_with_nodestack.view_changer.view_no = node_with_nodestack.view_changer.view_no + 1
    node_with_nodestack.view_changer._accepted_view_change_done_message = ('someNode',
                                                                           node_with_nodestack.ledger_summary)
    node_with_nodestack.send_current_state_to_lagging_node('Node2')


def test_vcdone_in_CS_with_valid_view_no_from_get_msgs_for_lagged_nodes(node_with_nodestack):
    """
    Check that ViewChangeDone messages has last completed viewNo
    view_changer.view_no is not usually completed viewNo. When view change in progress it would be propagated view number
    """
    fake_view_changer = node_with_nodestack.view_changer
    fake_view_changer.get_msgs_for_lagged_nodes = functools.partial(ViewChanger.get_msgs_for_lagged_nodes,
                                                                    fake_view_changer)
    fake_view_changer._accepted_view_change_done_message = ('someNode',
                                                            node_with_nodestack.ledger_summary)
    vch_messages = fake_view_changer.get_msgs_for_lagged_nodes()
    for msg in vch_messages:
        assert msg.viewNo == fake_view_changer.last_completed_view_no


def test_vcdone_in_CS_with_valid_view_no_from_get_msgs_for_lagged_nodes_vc_in_progress(node_with_nodestack):
    """
    Check that ViewChangeDone messages has last completed viewNo.
    view_changer.view_no is not usually completed viewNo. When view change in progress it would be propagated view number
    """
    node_with_nodestack.view_changer.view_change_in_progress = True
    node_with_nodestack.view_changer.view_no = node_with_nodestack.view_changer.view_no + 1
    fake_view_changer = node_with_nodestack.view_changer
    fake_view_changer.get_msgs_for_lagged_nodes = functools.partial(ViewChanger.get_msgs_for_lagged_nodes,
                                                                    fake_view_changer)
    fake_view_changer._accepted_view_change_done_message = ('someNode',
                                                            node_with_nodestack.ledger_summary)
    vch_messages = fake_view_changer.get_msgs_for_lagged_nodes()
    for msg in vch_messages:
        assert msg.viewNo == fake_view_changer.last_completed_view_no


def test_get_msgs_for_lagged_node_if_no_accepted(node_with_nodestack):
    """
    If  _accepted_view_change_done_message is empty, then ViewChangeDone messages will be compiled from _view_change_done dict
    """
    fake_view_changer = node_with_nodestack.view_changer
    fake_view_changer._accepted_view_change_done_message = ()
    current_view = fake_view_changer.last_completed_view_no
    fake_view_changer._view_change_done[node_with_nodestack.name] = ('SomeNewPrimary',
                                                                    node_with_nodestack.ledger_summary)
    vch_messages = fake_view_changer.get_msgs_for_lagged_nodes()
    assert vch_messages == [ViewChangeDone(current_view,
                                           *fake_view_changer._view_change_done[node_with_nodestack.name])]


def test_get_msgs_for_lagged_node_if_no_accepted_vc_in_progress(node_with_nodestack):
    """
    If  _accepted_view_change_done_message is empty, then ViewChangeDone messages will be compiled from _view_change_done dict
    """
    current_view = node_with_nodestack.view_changer.last_completed_view_no
    node_with_nodestack.view_changer.view_no = current_view + 1
    node_with_nodestack.view_changer.view_change_in_progress = True
    fake_view_changer = node_with_nodestack.view_changer
    fake_view_changer._accepted_view_change_done_message = ()
    fake_view_changer._view_change_done[node_with_nodestack.name] = ('SomeNewPrimary',
                                                                    node_with_nodestack.ledger_summary)
    vch_messages = fake_view_changer.get_msgs_for_lagged_nodes()
    assert vch_messages == [ViewChangeDone(current_view,
                                           *fake_view_changer._view_change_done[node_with_nodestack.name])]
