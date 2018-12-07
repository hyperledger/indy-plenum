import functools
import json
from collections import deque

import pytest

from plenum.common.constants import PreVCStrategies
from plenum.common.messages.node_messages import Prepare, Commit
from plenum.common.util import compare_3PC_keys
from plenum.server.view_change.view_changer import ViewChanger
from plenum.test.delayers import vcd_delay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.test_node import TestNode
from plenum.test.view_change.helper import ensure_view_change

REQ_COUNT = 10
REQ_COUNT_AFTER_SLOW = 1


@pytest.fixture(scope='module')
def tconf(tconf):
    old_max_3pc = tconf.Max3PCBatchSize
    old_batch_wait = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = 1
    tconf.Max3PCBatchWait = 1000
    tconf.PRE_VC_STRATEGY = PreVCStrategies.VC_START_MSG_STRATEGY

    yield tconf

    tconf.Max3PCBatchSize = old_max_3pc
    tconf.Max3PCBatchWait = old_batch_wait
    del tconf.PRE_VC_STRATEGY


stashed_msgs = deque()


def not_processing_prepare(node):
    async def processNodeInBoxWithoutPrepare(self):
        """
        Process the messages in the node inbox asynchronously.
        """
        def get_ident(nodestack, node_name):
            ha = nodestack.remotes.get(node_name)
            for k, v in nodestack.remotesByKeys.items():
                if v == ha:
                    return k
        self.nodeIbStasher.process()
        for i in range(len(self.nodeInBox)):
            m = self.nodeInBox.popleft()
            if isinstance(m, tuple) and len(
                m) == 2 and not hasattr(m, '_field_types') and \
                    (isinstance(m[0], Prepare) or isinstance(m[0], Commit)):
                stashed_msgs.append((json.dumps(m[0]._asdict()),
                                     get_ident(self.nodestack, m[1])))
                continue
            await self.process_one_node_message(m)

    node.processNodeInBox = functools.partial(processNodeInBoxWithoutPrepare, node)


def test_prepare_in_queue_before_vc(looper,
                                    txnPoolNodeSet,
                                    sdk_wallet_steward,
                                    sdk_pool_handle):
    """
    Test steps:
    1. Sent N random requests.
    2. Patching processNodeInBox method for node Delta.
       This method will process only not Prepare messages and store in nodeInBox queue Prepare messages
    3. Sent one request and check, that all Prepares are stored in nodeInBox queue and there is quorum of it
    4. Compare last_ordered_3pc_key and last_prepared_certificate. Last_prepared_certificate must be greater then last ordered
    5. ppSeqNo in last_prepared_certificate must be at least as ppSeqNo for queued Prepares msgs in nodeInBox queue
    """
    def chk_quorumed_prepares_count(prepares, count):
        pp_qourum = slow_node.quorums.prepare.value
        assert len([pp for key, pp in prepares.items() if prepares.hasQuorum(pp.msg, pp_qourum)]) == count


    def patched_startViewChange(self, *args, **kwargs):
        self.node.processNodeInBox = functools.partial(TestNode.processNodeInBox, self.node)
        ViewChanger.startViewChange(self, *args, **kwargs)
        while stashed_msgs:
            self.node.nodestack.rxMsgs.append(stashed_msgs.popleft())

    """Send REQ_COUNT txns"""
    slow_node = txnPoolNodeSet[-1]
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, REQ_COUNT)
    """Check that there is REQ_COUNT prepares with quorum in queue"""
    chk_quorumed_prepares_count(slow_node.master_replica.prepares, REQ_COUNT)
    """Patch processNodeInBox method for saving Prepares in nodeInBox queue"""
    not_processing_prepare(slow_node)

    """Send 1 txn"""
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, REQ_COUNT_AFTER_SLOW)

    chk_quorumed_prepares_count(slow_node.master_replica.prepares, REQ_COUNT)

    """Get last ordered 3pc key (should be (0, REQ_COUNT))"""
    ordered_lpc = slow_node.master_replica.last_ordered_3pc
    """Delay view_change_done messages"""
    slow_node.nodeIbStasher.delay(vcd_delay(100))
    """Patch on_view_change_start method for reverting processNodeInBox method"""
    slow_node.view_changer.startViewChange = functools.partial(patched_startViewChange, slow_node.view_changer)
    """Initiate view change"""
    ensure_view_change(looper, txnPoolNodeSet)
    """Last prepared certificate should take into account Prepares in nodeInBox queue too"""
    expected_lpc = slow_node.master_replica.last_prepared_before_view_change
    assert expected_lpc == (0, 11)
    """Last ordered key should be less than last_prepared_before_view_change"""
    assert compare_3PC_keys(ordered_lpc, expected_lpc) > 0
