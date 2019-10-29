from collections import defaultdict

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO, \
    AUDIT_TXN_LEDGERS_SIZE
from plenum.test.instances.helper import recvd_prepares
from plenum.test.node_request.test_timestamp.helper import \
    get_timestamp_suspicion_count, make_clock_faulty
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.common.txn_util import get_txn_time, get_payload_data

from plenum.test.helper import sdk_send_random_and_check


@pytest.fixture(scope="module")
def tconf(tconf):
    oldMax3PCBatchSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 2
    yield tconf
    tconf.Max3PCBatchSize = oldMax3PCBatchSize


def test_replicas_prepare_time(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    last_domain_seq_no = txnPoolNodeSet[0].domainLedger.size + 1

    # Check that each replica's PREPARE time is same as the PRE-PREPARE time
    sent_batches = 5
    for i in range(sent_batches):
        sdk_send_random_and_check(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  count=2)
        looper.runFor(1)

    for node in txnPoolNodeSet:
        for r in node.replicas.values():
            rec_prps = defaultdict(list)
            for p in recvd_prepares(r):
                rec_prps[(p.viewNo, p.ppSeqNo)].append(p)
            pp_coll = r._ordering_service.sent_preprepares if r.isPrimary else r._ordering_service.prePrepares
            for key, pp in pp_coll.items():
                for p in rec_prps[key]:
                    assert pp.ppTime == p.ppTime

            # `last_accepted_pre_prepare_time` is the time of the last PRE-PREPARE
            assert r._ordering_service.last_accepted_pre_prepare_time == pp_coll.peekitem(-1)[
                1].ppTime

            # The ledger should store time for each txn and it should be same
            # as the time for that PRE-PREPARE
            if not r.isMaster:
                continue

            for _, audit_txn in node.auditLedger.getAllTxn():
                audit_txn_data = get_payload_data(audit_txn)
                three_pc_key = (audit_txn_data[AUDIT_TXN_VIEW_NO], audit_txn_data[AUDIT_TXN_PP_SEQ_NO])
                domain_seq_no = audit_txn_data[AUDIT_TXN_LEDGERS_SIZE][DOMAIN_LEDGER_ID]
                for seq_no in range(last_domain_seq_no, domain_seq_no + 1):
                    assert get_txn_time(node.domainLedger.getBySeqNo(seq_no)) \
                           == pp_coll[three_pc_key].ppTime
                last_domain_seq_no = domain_seq_no + 1


def test_non_primary_accepts_pre_prepare_time(looper, txnPoolNodeSet,
                                              sdk_wallet_client, sdk_pool_handle):
    """
    One of the non-primary has an in-correct clock so it thinks PRE-PREPARE
    has incorrect time
    """
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              count=2)
    # send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 2)
    # The replica having the bad clock
    confused_npr = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1]

    make_clock_faulty(confused_npr.node)

    old_acceptable_rvs = getAllReturnVals(
        confused_npr._ordering_service, confused_npr._ordering_service._is_pre_prepare_time_acceptable)
    old_susp_count = get_timestamp_suspicion_count(confused_npr.node)
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              count=2)

    assert get_timestamp_suspicion_count(confused_npr.node) > old_susp_count

    new_acceptable_rvs = getAllReturnVals(
        confused_npr._ordering_service, confused_npr._ordering_service._is_pre_prepare_time_acceptable)

    # `is_pre_prepare_time_acceptable` first returned False then returned True
    assert [True, False, *old_acceptable_rvs] == new_acceptable_rvs
