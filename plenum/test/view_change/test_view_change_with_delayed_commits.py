import pytest

from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_random_requests, sdk_get_replies, sdk_send_random_and_check, logger
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    old_unsafe = tconf.unsafe
    tconf.unsafe.add("disable_view_change")
    yield tconf
    tconf.unsafe = old_unsafe


def test_view_change_with_delayed_commits(txnPoolNodeSet, looper,
                                          sdk_pool_handle,
                                          sdk_wallet_client,
                                          tconf):

    def last_prepared_certificate(nodes, num):
        for n in nodes:
            assert n.master_replica.last_prepared_certificate_in_view() == num

    def view_change_done(nodes, view_no):
        for n in nodes:
            assert n.master_replica.viewNo == view_no
            assert n.master_replica.last_prepared_before_view_change is None

    majority_nodes = txnPoolNodeSet[:3]
    minority_nodes = txnPoolNodeSet[3:]
    majority_stashers = [n.nodeIbStasher for n in majority_nodes]
    minority_stashers = [n.nodeIbStasher for n in minority_nodes]

    with delay_rules(majority_stashers, cDelay()):
        with delay_rules(minority_stashers, cDelay()):
            requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                                sdk_wallet_client, 1)

            looper.run(eventually(last_prepared_certificate, txnPoolNodeSet, (0, 1)))

            for n in txnPoolNodeSet:
                n.view_changer.on_master_degradation()

        # Now commits are processed on minority of nodes wait until view change is done
        looper.run(eventually(view_change_done, txnPoolNodeSet, 1, timeout=60))

    sdk_get_replies(looper, requests)


    majority_nodes = txnPoolNodeSet[1:]
    minority_nodes = txnPoolNodeSet[:1]
    majority_stashers = [n.nodeIbStasher for n in majority_nodes]
    minority_stashers = [n.nodeIbStasher for n in minority_nodes]

    with delay_rules(majority_stashers, cDelay()):
        with delay_rules(minority_stashers, cDelay()):
            requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                                sdk_wallet_client, 1)

            looper.run(eventually(last_prepared_certificate, txnPoolNodeSet[:3], (1, 2)))

            for n in txnPoolNodeSet:
                n.view_changer.on_master_degradation()

        # Now commits are processed on minority of nodes wait until view change is done
        looper.run(eventually(view_change_done, txnPoolNodeSet, 2, timeout=60))

    sdk_get_replies(looper, requests)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 4)
