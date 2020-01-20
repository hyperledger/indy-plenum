from typing import Iterable

from plenum.common.util import max_3PC_key, getNoInstances, getMaxFailures
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.delayers import icDelay, cDelay, pDelay, nv_delay
from plenum.test.helper import sdk_send_random_request, sdk_get_reply, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules, delay_rules_without_processing
from plenum.test.test_node import getRequiredInstances, ensureElectionsDone, checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually, eventuallyAll
from stp_core.loop.looper import Looper


def last_prepared_certificate(nodes):
    """
    Find last prepared certificate in pool.
    When we don't have any request ordered in new view last_prepared_certificate_in_view()
    returns None, but in order to ease maths (like being able to use max_3PC_key, or calculating
    next expected 3PC key) this value is replaced with (view_no, 0).
    """

    def patched_last_prepared_certificate(n):
        result = n.master_replica._ordering_service.l_last_prepared_certificate_in_view()
        if result is None:
            result = (n.master_replica.viewNo, 0)
        return result

    return max_3PC_key(patched_last_prepared_certificate(n) for n in nodes)


def check_last_prepared_certificate_on_quorum(nodes, num):
    # Check that last_prepared_certificate reaches some 3PC key on N-f nodes
    n = len(nodes)
    f = getMaxFailures(n)
    assert sum(1 for n in nodes
               if n.master_replica._ordering_service.l_last_prepared_certificate_in_view() == num) >= n - f


def check_last_prepared_certificate(nodes, num):
    # Check that last_prepared_certificate reaches some 3PC key on all nodes
    for n in nodes:
        assert n.master_replica._ordering_service.l_last_prepared_certificate_in_view() == num


def check_last_prepared_certificate_after_view_change_start(nodes, num):
    # Check that last_prepared_certificate reaches some 3PC key on all nodestest_slow_node_reverts_unordered_state_during_catchup
    for n in nodes:
        assert n.master_replica._consensus_data.prev_view_prepare_cert == num


def check_view_change_done(nodes, view_no):
    # Check that view change is done and view_no is not less than target
    for n in nodes:
        assert n.master_replica.viewNo >= view_no
        assert not n.master_replica._consensus_data.waiting_for_new_view


def check_last_ordered(nodes, num):
    for node in nodes:
        assert node.master_replica.last_ordered_3pc == num


def wait_for_elections_done_on_given_nodes(looper: Looper,
                                           nodes: Iterable[Node],
                                           num_of_instances: int,
                                           timeout: float,
                                           retry_wait: float=1.0):
    """
    Wait for primary elections to be completed on all the replicas
    of the given nodes.
    """
    def check_num_of_replicas():
        for node in nodes:
            assert len(node.replicas) == num_of_instances

    def verify_each_replica_knows_its_primary():
        for node in nodes:
            for inst_id, replica in node.replicas.items():
                assert replica.hasPrimary
                assert not replica._consensus_data.waiting_for_new_view

    looper.run(eventuallyAll(check_num_of_replicas,
                             verify_each_replica_knows_its_primary,
                             totalTimeout=timeout,
                             retryWait=retry_wait))


def do_view_change_with_pending_request_and_one_fast_node(fast_node,
                                                          nodes, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Perform view change while processing request, with one node receiving commits much sooner than others.
    With current implementation of view change this will result in corrupted state of fast node
    """

    fast_stasher = fast_node.nodeIbStasher

    slow_nodes = [n for n in nodes if n != fast_node]
    slow_stashers = [n.nodeIbStasher for n in slow_nodes]

    # Get last prepared certificate in pool
    lpc = last_prepared_certificate(nodes)
    # Get pool current view no
    view_no = lpc[0]

    # Delay all COMMITs
    with delay_rules(slow_stashers, cDelay()):
        with delay_rules(fast_stasher, cDelay()):
            # Send request
            request = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

            # Wait until this request is prepared on N-f nodes
            looper.run(eventually(check_last_prepared_certificate_on_quorum, nodes, (lpc[0], lpc[1] + 1)))

            # Trigger view change
            trigger_view_change(nodes)

        # Now commits are processed on fast node
        # Wait until view change is complete
        looper.run(eventually(check_view_change_done, nodes, view_no + 1, timeout=60))

    # Finish request gracefully
    sdk_get_reply(looper, request)


def do_view_change_with_unaligned_prepare_certificates(
        slow_nodes, nodes, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Perform view change with some nodes reaching lower last prepared certificate than others.
    With current implementation of view change this can result with view change taking a lot of time.
    """
    fast_nodes = [n for n in nodes if n not in slow_nodes]

    all_stashers = [n.nodeIbStasher for n in nodes]
    slow_stashers = [n.nodeIbStasher for n in slow_nodes]

    # Delay some PREPAREs and all COMMITs
    with delay_rules(slow_stashers, pDelay()):
        with delay_rules(all_stashers, cDelay()):
            # Send request
            request = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

            # Wait until this request is prepared on fast nodes
            looper.run(eventually(check_last_prepared_certificate, fast_nodes, (0, 1)))
            # Make sure its not prepared on slow nodes
            looper.run(eventually(check_last_prepared_certificate, slow_nodes, None))

            # Trigger view change
            trigger_view_change(nodes)

        # Now commits are processed
        # Wait until view change is complete
        looper.run(eventually(check_view_change_done, nodes, 1, timeout=60))

    # Finish request gracefully
    sdk_get_reply(looper, request)

    ensure_all_nodes_have_same_data(looper, nodes)
    sdk_ensure_pool_functional(looper, nodes, sdk_wallet_client, sdk_pool_handle)


def do_view_change_with_delay_on_one_node(slow_node, nodes, looper,
                                          sdk_pool_handle, sdk_wallet_client):
    slow_stasher = slow_node.nodeIbStasher

    fast_nodes = [n for n in nodes if n != slow_node]

    stashers = [n.nodeIbStasher for n in nodes]

    # Get last prepared certificate in pool
    lpc = last_prepared_certificate(nodes)
    # Get pool current view no
    view_no = lpc[0]

    with delay_rules(slow_stasher, nv_delay()):
        with delay_rules(slow_stasher, icDelay()):
            with delay_rules(stashers, cDelay()):
                # Send request
                request = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

                # Wait until this request is prepared on N-f nodes
                looper.run(eventually(check_last_prepared_certificate_on_quorum, nodes, (lpc[0], lpc[1] + 1)))

                # Trigger view change
                trigger_view_change(nodes)

                # Wait until view change is completed on all nodes except slow one
                waitForViewChange(looper,
                                  fast_nodes,
                                  expectedViewNo=view_no + 1,
                                  customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(nodes)))
            wait_for_elections_done_on_given_nodes(looper,
                                                   fast_nodes,
                                                   getRequiredInstances(len(nodes)),
                                                   timeout=waits.expectedPoolElectionTimeout(len(nodes)))

            # Now all the nodes receive Commits
            # The slow node will accept Commits and order the 3PC-batch in the old view
            looper.runFor(waits.expectedOrderingTime(getNoInstances(len(nodes))))

        # Now slow node receives InstanceChanges
        waitForViewChange(looper,
                          [slow_node],
                          expectedViewNo=view_no + 1,
                          customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(nodes)))

    # Now slow node receives NewView
    wait_for_elections_done_on_given_nodes(looper,
                                           [slow_node],
                                           getRequiredInstances(len(nodes)),
                                           timeout=waits.expectedPoolElectionTimeout(len(nodes)))

    # Finish request gracefully
    sdk_get_reply(looper, request)


def do_view_change_with_propagate_primary_on_one_delayed_node(
        slow_node, nodes, looper, sdk_pool_handle, sdk_wallet_client):

    slow_stasher = slow_node.nodeIbStasher

    fast_nodes = [n for n in nodes if n != slow_node]

    stashers = [n.nodeIbStasher for n in nodes]

    # Get last prepared certificate in pool
    lpc = last_prepared_certificate(nodes)
    # Get pool current view no
    view_no = lpc[0]

    with delay_rules(slow_stasher, icDelay()):
        with delay_rules(slow_stasher, nv_delay()):
            with delay_rules(stashers, cDelay()):
                # Send request
                request = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

                # Wait until this request is prepared on N-f nodes
                looper.run(eventually(check_last_prepared_certificate_on_quorum, nodes, (lpc[0], lpc[1] + 1)))

                # Trigger view change
                trigger_view_change(nodes)

                # Wait until view change is completed on all nodes except slow one
                waitForViewChange(looper,
                                  fast_nodes,
                                  expectedViewNo=view_no + 1,
                                  customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(nodes)))
            wait_for_elections_done_on_given_nodes(looper,
                                                   fast_nodes,
                                                   getRequiredInstances(len(nodes)),
                                                   timeout=waits.expectedPoolElectionTimeout(len(nodes)))

            # Now all the nodes receive Commits
            # The slow node will accept Commits and order the 3PC-batch in the old view
            looper.runFor(waits.expectedOrderingTime(getNoInstances(len(nodes))))

    # Now slow node receives NewView
    waitForViewChange(looper,
                      [slow_node],
                      expectedViewNo=view_no + 1,
                      customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(nodes)))
    wait_for_elections_done_on_given_nodes(looper,
                                           [slow_node],
                                           getRequiredInstances(len(nodes)),
                                           timeout=waits.expectedPoolElectionTimeout(len(nodes)))

    # Now slow node receives InstanceChanges but discards them because already
    # started propagate primary to the same view.

    # Finish request gracefully
    sdk_get_reply(looper, request)


def do_view_change_with_delayed_commits_and_node_restarts(fast_nodes, slow_nodes, nodes_to_restart,
                                                          old_view_no, old_last_ordered,
                                                          looper, sdk_pool_handle, sdk_wallet_client,
                                                          tconf, tdir, all_plugins_path,
                                                          wait_for_catchup=False):
    """
    Delays commits without processing on `slow_nodes`, restarts `nodes_to_restart`, triggers view change, and confirms
    that view changed completed successfully and that the ledgers are consistent and in sync.

    :param fast_nodes: Nodes that will order the requests
    :param slow_nodes: Nodes whose commits will be delay, and that will not order the requests
    :param nodes_to_restart: Nodes that will be restarted
    :param old_view_no: View that we started from
    :param old_last_ordered: Last ordered 3pc txn before we did any requests
    :param wait_for_catchup: Should we wait for restarted nodes to finish catchup
    """

    nodes = fast_nodes + slow_nodes

    slow_stashers = [slow_node.nodeIbStasher for slow_node in slow_nodes]

    # Delay commits on `slow_nodes`
    with delay_rules_without_processing(slow_stashers, cDelay()):

        request = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

        # Check that all of the nodes except the slows one ordered the request
        looper.run(eventually(check_last_ordered, fast_nodes, (old_view_no, old_last_ordered[1] + 1)))
        looper.run(eventually(check_last_ordered, slow_nodes, old_last_ordered))

    # Restart nodes
    for node in nodes_to_restart:
        disconnect_node_and_ensure_disconnected(
            looper,
            nodes,
            node,
            timeout=len(nodes_to_restart),
            stopNode=True
        )
        looper.removeProdable(node)
        nodes.remove(node)

        restarted_node = start_stopped_node(node, looper, tconf, tdir, all_plugins_path)
        nodes.append(restarted_node)

    looper.runFor(waits.expectedNodeStartUpTimeout())
    looper.run(checkNodesConnected(nodes))

    if wait_for_catchup:
        ensure_all_nodes_have_same_data(looper, nodes)

    # Trigger view change on all nodes
    trigger_view_change(nodes)

    assert len(nodes) == len(slow_nodes) + len(fast_nodes)

    # Assert that view change was successful and that ledger data is consistent
    waitForViewChange(
        looper,
        nodes,
        expectedViewNo=(old_view_no + 1),
        customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(nodes))
    )
    ensureElectionsDone(looper=looper, nodes=nodes)
    ensure_all_nodes_have_same_data(looper, nodes)
    sdk_get_reply(looper, request)
    sdk_ensure_pool_functional(looper, nodes, sdk_wallet_client, sdk_pool_handle)
