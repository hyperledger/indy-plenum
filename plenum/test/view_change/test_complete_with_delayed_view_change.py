import functools
from collections import deque

import pytest

from plenum.common.constants import PreVCStrategies
from plenum.common.messages.node_messages import ViewChangeDone, InstanceChange
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import TestNode, ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

REQ_COUNT = 10
stashed_vc_done_msgs = deque()
stashed_ic_msgs = deque()
got_start_vc_msg = False


@pytest.fixture(scope="module")
def tconf(tconf):
    tconf.PRE_VC_STRATEGY = PreVCStrategies.VC_START_MSG_STRATEGY
    yield tconf
    del tconf.PRE_VC_STRATEGY


def not_processing_view_change_done(node):
    async def processNodeInBoxWithoutVCDone(self):
        """
        Process the messages in the node inbox asynchronously.
        """
        self.nodeIbStasher.process()
        for i in range(len(self.nodeInBox)):
            m = self.nodeInBox.popleft()
            if isinstance(m, tuple) and len(
                m) == 2 and not hasattr(m, '_field_types') and \
                    isinstance(m[0], (ViewChangeDone, InstanceChange)) and \
                    m[0].viewNo > self.viewNo:
                if isinstance(m[0], ViewChangeDone):
                    stashed_vc_done_msgs.append(m)
                else:
                    stashed_ic_msgs.append(m)
                continue
            await self.process_one_node_message(m)

    node.processNodeInBox = functools.partial(processNodeInBoxWithoutVCDone, node)


def test_complete_with_delayed_view_change(looper,
                                           txnPoolNodeSet,
                                           sdk_wallet_steward,
                                           sdk_pool_handle):
    def chk_len_stashed_msgs():
        assert len(stashed_vc_done_msgs) >= slow_node.quorums.view_change_done.value - 1

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, REQ_COUNT)
    slow_node = txnPoolNodeSet[-1]
    not_processing_view_change_done(slow_node)
    ensure_view_change(looper, txnPoolNodeSet[:-1])
    looper.run(eventually(chk_len_stashed_msgs))
    while stashed_ic_msgs:
        slow_node.nodeInBox.append(stashed_ic_msgs.popleft())
    while stashed_vc_done_msgs:
        slow_node.nodeInBox.append(stashed_vc_done_msgs.popleft())
    slow_node.processNodeInBox = functools.partial(TestNode.processNodeInBox, slow_node)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
