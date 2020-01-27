from contextlib import contextmanager

from orderedset._orderedset import OrderedSet

from plenum.common.constants import STEWARD_STRING
from plenum.test.helper import sdk_send_random_request, get_key_from_req, sdk_get_and_check_replies, \
    sdk_send_random_and_check
from plenum.test.pool_transactions.helper import prepare_node_request, \
    sdk_sign_and_send_prepared_request, sdk_add_new_nym, prepare_new_node_data
from stp_core.loop.eventually import eventually


@contextmanager
def ord_delay(nodes):
    delay_msgs = dict()
    processing_methods = dict()
    for n in nodes:
        delay_msgs.setdefault(n.name, OrderedSet())
        processing_methods[n.name] = n.processOrdered
        n.processOrdered = lambda msg: delay_msgs[n.name].add(msg)

    yield

    for n in nodes:
        n.processOrdered = processing_methods[n.name]
        for msg in delay_msgs[n.name]:
            n.processOrdered(msg)


def test_commit_signature_validation_integration(looper,
                                                 txnPoolNodeSet,
                                                 sdk_pool_handle,
                                                 sdk_wallet_steward,
                                                 sdk_wallet_client,
                                                 tconf,
                                                 tdir):
    '''
    All nodes receive PrePrepare1(txn1 for pool_ledger)
    Nodes 1, 2 ordered txn1 and nodes 3, 4 did not.
    All nodes  receive PrePrepare2(txn2 for domain_ledger)
    Nodes 3, 4 receive commits from nodes 1, 2
    Nodes 3, 4 ordered txn1
    Check that all nodes ordered txn2
    '''
    fast_nodes = txnPoolNodeSet[:2]
    slow_nodes = txnPoolNodeSet[2:]

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 1)

    # create new steward
    new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                sdk_pool_handle,
                                                sdk_wallet_steward,
                                                alias="testClientSteward945",
                                                role=STEWARD_STRING)

    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, "new_node")

    # create node request to add new demote node
    _, steward_did = new_steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name="new_node",
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=[],
                             key_proof=key_proof))

    first_ordered = txnPoolNodeSet[0].master_last_ordered_3PC
    with ord_delay(slow_nodes):
        request1 = sdk_sign_and_send_prepared_request(looper, new_steward_wallet_handle,
                                                      sdk_pool_handle, node_request)

        key1 = get_key_from_req(request1[0])

        def check_nodes_receive_pp(view_no, seq_no):
            for node in txnPoolNodeSet:
                assert node.master_replica._ordering_service.get_preprepare(view_no, seq_no)

        looper.run(eventually(check_nodes_receive_pp, first_ordered[0], first_ordered[1] + 1))

        def check_fast_nodes_ordered_request():
            for n in fast_nodes:
                assert key1 not in n.requests or n.requests[key1].executed
            for n in slow_nodes:
                assert not n.requests[key1].executed

        looper.run(eventually(check_fast_nodes_ordered_request))

        request2 = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)
        looper.run(eventually(check_nodes_receive_pp, first_ordered[0], first_ordered[1] + 2))

        def check_nodes_receive_commits(view_no, seq_no):
            for node in txnPoolNodeSet:
                assert len(node.master_replica._ordering_service.commits[view_no, seq_no].voters) >= node.f + 1
        looper.run(eventually(check_nodes_receive_commits, first_ordered[0], first_ordered[1] + 2))

    sdk_get_and_check_replies(looper, [request1])
    sdk_get_and_check_replies(looper, [request2])
