import pytest
from plenum.common.constants import PREPREPARE
from plenum.common.messages.node_messages import PrePrepare
from plenum.test.helper import sdk_send_random_requests, sdk_get_and_check_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from stp_core.common.log import getlogger
from plenum.test.delayers import msg_rep_delay, cr_delay


logger = getlogger()

@pytest.fixture(scope="module")
def tconf(tconf):
   tconf.Max3PCBatchSize = 10
   tconf.CHK_FREQ = 3
   tconf.CatchupTransactionsTimeout = 5
   return tconf


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 150


def drop_preprepares(node):
   def method_ignore(msg, frm):
       logger.error("{} ignores PrePrepare".format(node.name))

   node.nodeMsgRouter.extend([(PrePrepare, method_ignore)])


def gen_cr_delay(nodes, conf):
   return len(nodes) * conf.CatchupTransactionsTimeout


def prepare_nodes(nodes, crd):
   drop_preprepares(nodes[-1])
   i = 0
   for n in nodes:
       n.nodeIbStasher.delay(msg_rep_delay(400, [PREPREPARE,]))
       n.nodeIbStasher.delay(cr_delay(1 + i))
       i += 1


def test_catchup_repls_overlapping(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward,
                                  tconf, limitTestRunningTime):

   assert len(txnPoolNodeSet) == 4
   crd = gen_cr_delay(txnPoolNodeSet, tconf)
   prepare_nodes(txnPoolNodeSet, crd)

   reqs = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_steward, 100)
   looper.runFor(1)
   sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_steward, 100)
   looper.runFor(1)
   sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_steward, 100)
   looper.runFor(1)
   reqs1 = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_steward, 100)

   # looper.runFor(crd)

   sdk_get_and_check_replies(looper, reqs)

   sdk_get_and_check_replies(looper, reqs1, timeout=20)

   ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet, custom_timeout=60)
   assert len(txnPoolNodeSet[-1].nodeBlacklister.blacklisted) == 0
