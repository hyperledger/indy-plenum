import functools
import pytest

from plenum.common.constants import PreVCStrategies
from plenum.common.messages.node_messages import InstanceChange, ViewChangeStartMessage
from plenum.server.node import Node
from plenum.test.delayers import icDelay, vcd_delay
from plenum.test.view_change.helper import ensure_view_change


@pytest.fixture(scope="module")
def tconf(tconf):
    tconf.PRE_VC_STRATEGY = PreVCStrategies.VC_START_MSG_STRATEGY
    yield tconf
    del tconf.PRE_VC_STRATEGY


def test_add_vc_start_msg_during_start_view_change(txnPoolNodeSet,
                                                   looper):
    delayed_node = txnPoolNodeSet[-1]
    current_view_no = delayed_node.viewNo
    proposed_view_no = current_view_no + 1
    delayed_node.nodeIbStasher.delay(vcd_delay(1000))
    delayed_node.nodeIbStasher.delay(icDelay(1000))
    assert delayed_node.view_changer
    ensure_view_change(looper, txnPoolNodeSet[:-1])
    """
    If view number was incremented, that means that instanceChange quorum was archived 
    """
    assert txnPoolNodeSet[0].viewNo == proposed_view_no
    looper.removeProdable(delayed_node)
    delayed_node.nodeInBox.append((InstanceChange(1, 25), 'Alpha'))
    delayed_node.nodeInBox.append((InstanceChange(1, 25), 'Beta'))
    delayed_node.nodeInBox.append((InstanceChange(1, 25), 'Gamma'))
    delayed_node.processNodeInBox = functools.partial(Node.processNodeInBox, delayed_node)
    looper.run(delayed_node.processNodeInBox())
    looper.run(delayed_node.serviceViewChanger(None))
    assert len(delayed_node.nodeInBox) == 1
    m = delayed_node.nodeInBox.popleft()
    assert isinstance(m[0], ViewChangeStartMessage)
