import pytest
import time

from plenum.common.messages.node_messages import PrePrepare, Prepare
from plenum.test.test_node import getNonPrimaryReplicas
from stp_core.common.log import getlogger

logger = getlogger()


def test_setup_for_non_master_after_catchup(txnPoolNodeSet,
                                            sdk_wallet_client):
    inst_id = 1
    replica = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1]
    replica.last_ordered_3pc = (0, 0)
    timestamp = time.time()
    ppSeqNo = 5
    preprepare, prepare = \
        _create_prepare_and_preprepare(inst_id,
                                       replica.viewNo,
                                       ppSeqNo,
                                       timestamp,
                                       sdk_wallet_client)
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] = \
        (preprepare, replica.primaryName)
    for node in txnPoolNodeSet:
        replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] = \
            (prepare, node.name)
    replica._setup_for_non_master(is_catchup=False)
    assert replica.last_ordered_3pc == (replica.viewNo, ppSeqNo - 1)


def test_setup_for_non_master_in_catchup(txnPoolNodeSet,
                                         sdk_wallet_client):
    inst_id = 1
    replica = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1]
    timestamp = time.time()
    ppSeqNo = 5
    preprepare, prepare = \
        _create_prepare_and_preprepare(inst_id,
                                       replica.viewNo,
                                       ppSeqNo,
                                       timestamp,
                                       sdk_wallet_client)
    replica.prePreparesPendingPrevPP[replica.viewNo, ppSeqNo] = \
        (preprepare, replica.primaryName)
    for node in txnPoolNodeSet:
        replica.preparesWaitingForPrePrepare[replica.viewNo, ppSeqNo] = \
            (prepare, node.name)
    replica._setup_for_non_master()
    assert replica.last_ordered_3pc == (replica.viewNo, ppSeqNo - 1)


def test_setup_for_non_master_without_catchup(txnPoolNodeSet):
    inst_id = 1
    last_ordered_3pc = (5, 12)
    replica = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1]
    replica.last_ordered_3pc = last_ordered_3pc
    replica._setup_for_non_master(is_catchup=False)
    assert replica.last_ordered_3pc == last_ordered_3pc


def _create_prepare_and_preprepare(inst_id, pp_sq_no, view_no, timestamp,
                                   sdk_wallet_client):
    time = int(timestamp)
    req_idr = [[sdk_wallet_client[1], 1234]]
    preprepare = PrePrepare(inst_id,
                            pp_sq_no,
                            view_no,
                            time,
                            req_idr,
                            1,
                            "123",
                            1,
                            None,
                            None)
    prepare = Prepare(inst_id,
                      pp_sq_no,
                      view_no,
                      time,
                      "321",
                      None,
                      None
                      )
    return preprepare, prepare
