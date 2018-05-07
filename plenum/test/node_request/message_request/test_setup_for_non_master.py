import time
import pytest

from plenum.common.messages.node_messages import PrePrepare
from plenum.common.types import f
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check

logger = getlogger()


def test_setup_for_non_master(looper,
                              txnPoolNodeSet,
                              sdk_wallet_client,
                              sdk_pool_handle,
                              tconf,
                              tdirWithPoolTxns,
                              allPluginsPath,
                              monkeypatch):
    INIT_REQS_CNT = 10
    non_primary_node = txnPoolNodeSet[0].replicas[1]
    for node in txnPoolNodeSet:
        if not node.replicas[1].isPrimary:
            non_primary_node = node.replicas[1]
            break
    monkeypatch.setattr(non_primary_node, '_can_process_pre_prepare',
                        lambda x, y: None)
    monkeypatch.setattr(non_primary_node, '_apply_pre_prepare',
                        lambda x, y: None)

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              INIT_REQS_CNT)

    req_key = 12345
    msg = {
        f.INST_ID.nm: 1,
        f.VIEW_NO.nm: 0,
        f.PP_SEQ_NO.nm: 2,
        f.PP_TIME.nm: int(time.time()),
        f.REQ_IDR.nm: [[sdk_wallet_client[1], req_key]],
        f.DISCARDED.nm: 1,
        f.DIGEST.nm: "123",
        f.LEDGER_ID.nm: 1,
        f.STATE_ROOT.nm: None,
        f.TXN_ROOT.nm: None,
    }

    non_primary_node.dispatchThreePhaseMsg(PrePrepare(**msg),
                                           non_primary_node.primaryName)
    assert non_primary_node.last_ordered_3pc[1] == 1
    assert (0, 2) in non_primary_node.prePrepares.keys()
