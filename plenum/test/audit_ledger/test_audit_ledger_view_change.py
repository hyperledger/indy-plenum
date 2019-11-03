import pytest

from plenum.common.startable import Mode
from plenum.test.delayers import icDelay
from plenum.test.stasher import delay_rules

from plenum.common.constants import DOMAIN_LEDGER_ID, STEWARD_STRING
from plenum.test.audit_ledger.helper import check_audit_ledger_updated, check_audit_txn
from plenum.test.helper import sdk_send_random_and_check, assertExp, get_pp_seq_no
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_add_new_node
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone
from stp_core.loop.eventually import eventually

nodeCount = 6


@pytest.mark.skip(reason="INDY-2276. Issue with adding node that will change f value")
def test_audit_ledger_view_change(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward,
                                  initial_domain_size, initial_pool_size, initial_config_size,
                                  tdir,
                                  tconf,
                                  allPluginsPath,
                                  view_no, pp_seq_no,
                                  initial_seq_no,
                                  monkeypatch):
    '''
    1. Send a NODE transaction and add a 7th Node for adding a new instance,
    but delay Ordered messages.
    2. Send a NYM txn.
    3. Reset delays in executing force_process_ordered
    4. Check that an audit txn for the NYM txn uses primary list from uncommitted
    audit with a new list of primaries.
    '''
    expected_pp_seq_no = 0
    other_nodes = txnPoolNodeSet[:-1]
    slow_node = txnPoolNodeSet[-1]
    # Add a new steward for creating a new node
    new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                sdk_pool_handle,
                                                sdk_wallet_steward,
                                                alias="newSteward",
                                                role=STEWARD_STRING)

    audit_size_initial = [node.auditLedger.size for node in txnPoolNodeSet]

    ordereds = []
    monkeypatch.setattr(slow_node, 'try_processing_ordered', lambda msg: ordereds.append(msg))

    with delay_rules([n.nodeIbStasher for n in txnPoolNodeSet], icDelay()):

        # Send NODE txn fo 7th node
        new_node = sdk_add_new_node(looper,
                                    sdk_pool_handle,
                                    new_steward_wallet_handle,
                                    "Theta",
                                    tdir,
                                    tconf,
                                    allPluginsPath)

        txnPoolNodeSet.append(new_node)
        looper.run(checkNodesConnected(other_nodes + [new_node]))

        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_client, 1)

        check_audit_ledger_updated(audit_size_initial, [slow_node],
                                   audit_txns_added=0)
        looper.run(eventually(check_audit_ledger_uncommitted_updated,
                              audit_size_initial, [slow_node], 2))

        def patch_force_process_ordered():
            for msg in list(ordereds):
                slow_node.replicas[msg.instId].outBox.append(msg)
                ordereds.remove(msg)
            monkeypatch.undo()
            slow_node.force_process_ordered()

        assert ordereds
        monkeypatch.setattr(slow_node, 'force_process_ordered', patch_force_process_ordered)
        for n in txnPoolNodeSet:
            n.start_catchup()
        looper.run(eventually(lambda nodes: assertExp(all([n.mode == Mode.participating for n in nodes])), txnPoolNodeSet))

    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    looper.run(eventually(lambda: assertExp(not ordereds)))

    lpps = set([n.master_replica.lastPrePrepareSeqNo for n in txnPoolNodeSet])
    assert len(lpps) == 1
    expected_pp_seq_no = lpps.pop()

    for node in txnPoolNodeSet:
        last_txn = node.auditLedger.get_last_txn()
        last_txn['txn']['data']['primaries'] = node._get_last_audited_primaries()
        check_audit_txn(txn=last_txn,
                        view_no=view_no + 1, pp_seq_no=expected_pp_seq_no,
                        seq_no=initial_seq_no + 4,
                        txn_time=node.master_replica._ordering_service.last_accepted_pre_prepare_time,
                        txn_roots={DOMAIN_LEDGER_ID: node.getLedger(DOMAIN_LEDGER_ID).tree.root_hash},
                        state_roots={DOMAIN_LEDGER_ID: node.getState(DOMAIN_LEDGER_ID).committedHeadHash},
                        pool_size=initial_pool_size + 1, domain_size=initial_domain_size + 2,
                        config_size=initial_config_size,
                        last_pool_seqno=2,
                        last_domain_seqno=1,
                        last_config_seqno=None,
                        primaries=node.primaries,
                        node_reg=[n.name for n in txnPoolNodeSet])


def check_audit_ledger_uncommitted_updated(audit_size_initial, nodes, audit_txns_added):
    audit_size_after = [node.auditLedger.uncommitted_size for node in nodes]
    for i in range(len(nodes)):
        assert audit_size_after[i] == audit_size_initial[i] + audit_txns_added, \
            "{} != {}".format(audit_size_after[i], audit_size_initial[i] + audit_txns_added)
