from collections import deque

import pytest
import time

from plenum.common.messages.node_messages import PrePrepare, Prepare
from plenum.common.util import compare_3PC_keys
from plenum.test.helper import sdk_send_random_and_check, init_discarded
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.test_node import getNonPrimaryReplicas, checkNodesConnected
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()


def test_setup_last_ordered_for_non_master_after_catchup(txnPoolNodeSet,
                                                         sdk_wallet_client):
    inst_id = 1
    replica = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1]
    replica.preparesWaitingForPrePrepare.clear()
    replica.prePreparesPendingPrevPP.clear()
    replica.last_ordered_3pc = (0, 0)
    timestamp = time.time()
    ppSeqNo = 5
    preprepare, prepare = \
        _create_prepare_and_preprepare(inst_id,
                                       replica.viewNo,
                                       ppSeqNo,
                                       timestamp,
                                       sdk_wallet_client,
                                       frm=node.name)
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] = deque()
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] \
        .append((preprepare, replica.primaryName))
    replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] = deque()
    for node in txnPoolNodeSet:
        replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] \
            .append((prepare, prepare.frm_replica))
    replica.first_batch_after_catchup = True
    replica._setup_last_ordered_for_non_master()
    assert replica.last_ordered_3pc == (replica.viewNo, ppSeqNo - 1)


def test_setup_last_ordered_for_non_master_without_preprepare(txnPoolNodeSet,
                                                              sdk_wallet_client):
    inst_id = 1
    replica = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1]
    replica.preparesWaitingForPrePrepare.clear()
    replica.prePreparesPendingPrevPP.clear()
    replica.last_ordered_3pc = (0, 0)
    timestamp = time.time()
    ppSeqNo = 5
    preprepare, prepare = \
        _create_prepare_and_preprepare(inst_id,
                                       replica.viewNo,
                                       ppSeqNo,
                                       timestamp,
                                       sdk_wallet_client,
                                       frm=node.name)
    replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] = deque()
    for node in txnPoolNodeSet:
        replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] \
            .append((prepare, prepare.frm_replica))
    replica._setup_last_ordered_for_non_master()
    assert replica.last_ordered_3pc == (0, 0)


def test_setup_last_ordered_for_non_master_without_quorum_of_prepares(
        txnPoolNodeSet,
        sdk_wallet_client):
    inst_id = 1
    replica = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1]
    replica.preparesWaitingForPrePrepare.clear()
    replica.prePreparesPendingPrevPP.clear()
    replica.last_ordered_3pc = (0, 0)
    timestamp = time.time()
    ppSeqNo = 5
    preprepare, prepare = \
        _create_prepare_and_preprepare(inst_id,
                                       replica.viewNo,
                                       ppSeqNo,
                                       timestamp,
                                       sdk_wallet_client,
                                       frm=txnPoolNodeSet[-1].name)
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] = deque()
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] \
        .append((preprepare, replica.primaryName))
    replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] = deque()
    replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] \
        .append((prepare, prepare.frm_replica))
    replica._setup_last_ordered_for_non_master()
    assert replica.last_ordered_3pc == (0, 0)


def test_setup_last_ordered_for_non_master_for_master(txnPoolNodeSet,
                                                      sdk_wallet_client):
    inst_id = 0
    replica = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1]
    replica.preparesWaitingForPrePrepare.clear()
    replica.prePreparesPendingPrevPP.clear()
    replica.last_ordered_3pc = (0, 0)
    timestamp = time.time()
    ppSeqNo = 5
    preprepare, prepare = \
        _create_prepare_and_preprepare(inst_id,
                                       replica.viewNo,
                                       ppSeqNo,
                                       timestamp,
                                       sdk_wallet_client,
                                       frm=node.name)
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] = deque()
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] \
        .append((preprepare, replica.primaryName))
    replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] = deque()
    for node in txnPoolNodeSet:
        replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] \
            .append((prepare, prepare.frm_replica))
    replica._setup_last_ordered_for_non_master()
    assert replica.last_ordered_3pc == (0, 0)


def test_setup_last_ordered_for_non_master_without_catchup(txnPoolNodeSet,
                                                           sdk_wallet_client):
    inst_id = 1
    last_ordered_3pc = (0, 12)
    timestamp = time.time()
    ppSeqNo = 16
    replica = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1]
    replica.last_ordered_3pc = last_ordered_3pc
    replica.preparesWaitingForPrePrepare.clear()
    replica.prePreparesPendingPrevPP.clear()
    preprepare, prepare = \
        _create_prepare_and_preprepare(inst_id,
                                       replica.viewNo,
                                       ppSeqNo,
                                       timestamp,
                                       sdk_wallet_client,
                                       frm=node.name)
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] = deque()
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] \
        .append((preprepare, replica.primaryName))
    replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] = deque()
    for node in txnPoolNodeSet:
        replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] \
            .append((prepare, prepare.frm_replica))

    replica._setup_last_ordered_for_non_master()
    assert replica.last_ordered_3pc == last_ordered_3pc


def _create_prepare_and_preprepare(inst_id, pp_sq_no, view_no, timestamp,
                                   sdk_wallet_client, **kwargs):
    time = int(timestamp)
    req_idr = ["random request digest"]
    preprepare = PrePrepare(
        inst_id,
        pp_sq_no,
        view_no,
        time,
        req_idr,
        init_discarded(),
        "123",
        1,
        None,
        None,
        0,
        True,
        **kwargs
    )
    prepare = Prepare(
        inst_id,
        pp_sq_no,
        view_no,
        time,
        "321",
        None,
        None,
        **kwargs
    )
    return preprepare, prepare
