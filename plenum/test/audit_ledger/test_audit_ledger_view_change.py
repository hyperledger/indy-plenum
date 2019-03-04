from contextlib import contextmanager

from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID, STEWARD_STRING, VALIDATOR
from plenum.common.types import f
from plenum.test.audit_ledger.helper import check_audit_ledger_updated, check_audit_txn
from plenum.test.bls.helper import sdk_change_bls_key
from plenum.test.helper import sdk_send_random_and_check, assertExp, sdk_get_and_check_replies
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_add_new_node, prepare_new_node_data, \
    prepare_node_request, sdk_sign_and_send_prepared_request, create_and_start_new_node
from plenum.test.test_node import TestNode, checkNodesConnected
from stp_core.loop.eventually import eventually

nodeCount = 6


@contextmanager
def delay_ordered(node):
    ordereds = []
    old_processing = node.try_processing_ordered
    node.try_processing_ordered = lambda msg: ordereds.append(msg)
    yield node
    node.try_processing_ordered = old_processing
    for msg in ordereds:
        node.replicas[msg.instId].outBox.append(msg)


def _add_new_node(looper, sdk_pool_handle, steward_wallet_handle, tdir, tconf, allPluginsPath,
                  new_node_name):
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, new_node_name)

    # filling node request
    _, steward_did = steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=[VALIDATOR],
                             key_proof=key_proof))

    # sending request using 'sdk_' functions
    request_couple = sdk_sign_and_send_prepared_request(looper, steward_wallet_handle,
                                                        sdk_pool_handle, node_request)
    #
    # # waitng for replies
    # sdk_get_and_check_replies(looper, [request_couple])

    return create_and_start_new_node(looper, new_node_name, tdir, sigseed,
                                     (nodeIp, nodePort), (clientIp, clientPort),
                                     tconf, True, allPluginsPath,
                                     TestNode)


def test_audit_ledger_view_change(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward,
                                  initial_domain_size, initial_pool_size, initial_config_size,
                                  tdir,
                                  tconf,
                                  allPluginsPath,
                                  view_no, pp_seq_no,
                                  initial_seq_no):
    '''
    Order 2 domain txns, 2 pool txns, and then 1 domain txn
    Check that audit ledger is correctly updated in all cases
    '''
    print("!!!!!{}".format(txnPoolNodeSet[0].master_last_ordered_3PC))

    # Add a new steward for creating a new node
    new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                sdk_pool_handle,
                                                sdk_wallet_steward,
                                                alias="newSteward",
                                                role=STEWARD_STRING)

    audit_size_initial = [node.auditLedger.size for node in txnPoolNodeSet]

    # force_process_ordered_calls = slow_node.spylog.count(slow_node.force_process_ordered)

    # Send NODE txn fo 8th node
    new_node = _add_new_node(looper,
                             sdk_pool_handle,
                             new_steward_wallet_handle,
                             tdir,
                             tconf,
                             allPluginsPath,
                             "Theta")

    # txnPoolNodeSet.append(new_node)
    # # looper.run(checkNodesConnected(txnPoolNodeSet))

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)

    # check_audit_ledger_updated(audit_size_initial, [slow_node],
    #                            audit_txns_added=0)
    # check_audit_ledger_uncommitted_updated(audit_size_initial, [slow_node],
    #                                        audit_txns_added=2)
    for n in txnPoolNodeSet:
        n.view_changer.on_master_degradation()

    #
    # looper.run(eventually(
    #     lambda: assertExp(slow_node.spylog.count(slow_node.force_process_ordered) -
    #                       force_process_ordered_calls == 1)))

    for node in txnPoolNodeSet:
        check_audit_txn(txn=node.auditLedger.get_last_txn(),
                        view_no=view_no, pp_seq_no=pp_seq_no + 2,
                        seq_no=initial_seq_no + 2, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        ledger_id=POOL_LEDGER_ID,
                        txn_root=node.getLedger(POOL_LEDGER_ID).tree.root_hash,
                        state_root=node.getState(POOL_LEDGER_ID).committedHeadHash,
                        pool_size=initial_pool_size + 1, domain_size=initial_domain_size + 1,
                        config_size=initial_config_size,
                        last_pool_seqno=None,
                        last_domain_seqno=1,
                        last_config_seqno=None)


#
# # 1st domain txn
# sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
#                           sdk_wallet_client, 1)
# check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
#                            audit_txns_added=1)
# for node in txnPoolNodeSet:
#     check_audit_txn(txn=node.auditLedger.get_last_txn(),
#                     view_no=view_no, pp_seq_no=pp_seq_no + 1,
#                     seq_no=initial_seq_no + 1, txn_time=node.master_replica.last_accepted_pre_prepare_time,
#                     ledger_id=DOMAIN_LEDGER_ID,
#                     txn_root=node.getLedger(DOMAIN_LEDGER_ID).tree.root_hash,
#                     state_root=node.getState(DOMAIN_LEDGER_ID).committedHeadHash,
#                     pool_size=initial_pool_size, domain_size=initial_domain_size + 1,
#                     config_size=initial_config_size,
#                     last_pool_seqno=None,
#                     last_domain_seqno=None,
#                     last_config_seqno=None)
#
# # 2d pool txn
# sdk_change_bls_key(looper, txnPoolNodeSet,
#                    txnPoolNodeSet[3],
#                    sdk_pool_handle,
#                    sdk_wallet_stewards[3],
#                    check_functional=False)
# check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
#                            audit_txns_added=4)
# for node in txnPoolNodeSet:
#     check_audit_txn(txn=node.auditLedger.get_last_txn(),
#                     view_no=view_no, pp_seq_no=pp_seq_no + 4,
#                     seq_no=initial_seq_no + 4, txn_time=node.master_replica.last_accepted_pre_prepare_time,
#                     ledger_id=POOL_LEDGER_ID,
#                     txn_root=node.getLedger(POOL_LEDGER_ID).tree.root_hash,
#                     state_root=node.getState(POOL_LEDGER_ID).committedHeadHash,
#                     pool_size=initial_pool_size + 2, domain_size=initial_domain_size + 2,
#                     config_size=initial_config_size,
#                     last_pool_seqno=None,
#                     last_domain_seqno=2,
#                     last_config_seqno=None)
#
# # one more domain txn
# sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
#                           sdk_wallet_client, 1)
# check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
#                            audit_txns_added=5)
# for node in txnPoolNodeSet:
#     check_audit_txn(txn=node.auditLedger.get_last_txn(),
#                     view_no=view_no, pp_seq_no=pp_seq_no + 5,
#                     seq_no=initial_seq_no + 5, txn_time=node.master_replica.last_accepted_pre_prepare_time,
#                     ledger_id=DOMAIN_LEDGER_ID,
#                     txn_root=node.getLedger(DOMAIN_LEDGER_ID).tree.root_hash,
#                     state_root=node.getState(DOMAIN_LEDGER_ID).committedHeadHash,
#                     pool_size=initial_pool_size + 2, domain_size=initial_domain_size + 3,
#                     config_size=initial_config_size,
#                     last_pool_seqno=4,
#                     last_domain_seqno=None,
#                     last_config_seqno=None)


def check_audit_ledger_uncommitted_updated(audit_size_initial, nodes, audit_txns_added):
    audit_size_after = [node.auditLedger.uncommitted_size for node in nodes]
    for i in range(len(nodes)):
        assert audit_size_after[i] == audit_size_initial[i] + audit_txns_added, \
            "{} != {}".format(audit_size_after[i], audit_size_initial[i] + audit_txns_added)
