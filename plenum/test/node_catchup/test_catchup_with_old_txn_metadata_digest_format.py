from logging import getLogger
from typing import Iterable

from plenum.common.constants import TXN_PAYLOAD, TXN_PAYLOAD_METADATA, TXN_PAYLOAD_METADATA_DIGEST, \
    TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST
from plenum.common.txn_util import append_payload_metadata
from plenum.test.delayers import delay_3pc
from plenum.test.helper import sdk_send_random_requests, sdk_get_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules_without_processing
from stp_core.loop.eventually import eventually

import plenum.common.txn_util as txn_util

logger = getLogger()


def test_catchup_with_old_txn_metadata_digest_format(tdir, tconf,
                                                     looper,
                                                     txnPoolNodeSet,
                                                     sdk_pool_handle,
                                                     sdk_wallet_client,
                                                     monkeypatch):
    lagging_node = txnPoolNodeSet[-1]
    lagging_stasher = lagging_node.nodeIbStasher
    other_nodes = txnPoolNodeSet[:-1]

    # Utility
    def check_nodes_domain_ledger(nodes: Iterable, txn_count: int):
        for node in nodes:
            assert node.domainLedger.size >= txn_count

    # Patch payload metadata, note that it will prevent pool from sending adequate replies to clients
    def append_old_payload_metadata(
            txn, frm=None, req_id=None,
            digest=None, payload_digest=None, taa_acceptance=None):
        txn = append_payload_metadata(txn, frm, req_id, digest, payload_digest, taa_acceptance)
        metadata = txn[TXN_PAYLOAD][TXN_PAYLOAD_METADATA]
        del metadata[TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST]
        metadata[TXN_PAYLOAD_METADATA_DIGEST] = payload_digest
        return txn
    monkeypatch.setattr(txn_util, 'append_payload_metadata', append_old_payload_metadata)

    # Check pool initial state
    initial_size = txnPoolNodeSet[0].domainLedger.size
    for node in txnPoolNodeSet:
        assert node.domainLedger.size == initial_size

    # Order some transactions, with one node discarding messages
    with delay_rules_without_processing(lagging_stasher, delay_3pc()):
        reps = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 10)
        looper.run(eventually(check_nodes_domain_ledger, other_nodes, initial_size + 10))
        assert lagging_node.domainLedger.size == initial_size

    # Catchup transactions and ensure that all nodes will eventually have same data
    lagging_node.start_catchup()
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # Catch replies
    sdk_get_replies(looper, reps)
