import pytest

from plenum.common.constants import LEDGER_STATUS, COMMIT
from plenum.common.messages.node_messages import MessageRep, ConsistencyProof
from plenum.test.delayers import delay_3pc, lsDelay, msg_rep_delay, cpDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules_without_processing
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    tmp = tconf.LedgerStatusTimeout

    tconf.LedgerStatusTimeout = 0.5

    yield tconf
    tconf.LedgerStatusTimeout = tmp


def test_catchup_with_reask_ls(txnPoolNodeSet,
                               looper,
                               sdk_pool_handle,
                               sdk_wallet_steward,
                               tconf,
                               tdir,
                               allPluginsPath):
    '''
    Start a catchup
    Delay MessageqResp with LedgerStatuses twice
    Check that the catchup finished
    '''
    lagged_node = txnPoolNodeSet[-1]
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)
    lagged_node.nodeIbStasher.delay(msg_rep_delay(types_to_delay=[COMMIT]))

    with delay_rules_without_processing(lagged_node.nodeIbStasher, delay_3pc(),
                                        msg_rep_delay(types_to_delay=[COMMIT])):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward,
                                  2)
        lagged_node.nodeIbStasher.drop_delayeds()
    with delay_rules_without_processing(lagged_node.nodeIbStasher,
                                        lsDelay(),
                                        msg_rep_delay(types_to_delay=[LEDGER_STATUS])):
        lagged_node.start_catchup()

        def chk():
            resp_ls_count = 0
            for msg in lagged_node.nodeIbStasher.delayeds:
                if isinstance(msg.item[0], MessageRep) and msg.item[0].msg_type == LEDGER_STATUS:
                    resp_ls_count += 1
            assert resp_ls_count >= (len(txnPoolNodeSet) - 1) * 2
            lagged_node.nodeIbStasher.drop_delayeds()

        looper.run(eventually(chk))
    waitNodeDataEquality(looper, lagged_node, *txnPoolNodeSet,
                         exclude_from_check=['check_last_ordered_3pc_backup'])


def test_catchup_with_reask_cp(txnPoolNodeSet,
                               looper,
                               sdk_pool_handle,
                               sdk_wallet_steward,
                               tconf,
                               tdir,
                               allPluginsPath):
    '''
    Start a catchup
    Delay ConsistencyProofs twice
    Check that the catchup finished
    '''
    lagged_node = txnPoolNodeSet[-1]
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)
    with delay_rules_without_processing(lagged_node.nodeIbStasher, delay_3pc(),
                                        msg_rep_delay(types_to_delay=[COMMIT])):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward,
                                  2)
        lagged_node.nodeIbStasher.drop_delayeds()

    with delay_rules_without_processing(lagged_node.nodeIbStasher, cpDelay()):
        lagged_node.start_catchup()

        def chk():
            cp_count = 0
            for msg in lagged_node.nodeIbStasher.delayeds:
                if isinstance(msg.item[0], ConsistencyProof):
                    cp_count += 1
            assert cp_count >= (len(txnPoolNodeSet) - 1) * 2
            lagged_node.nodeIbStasher.drop_delayeds()

        looper.run(eventually(chk))
    waitNodeDataEquality(looper, lagged_node, *txnPoolNodeSet,
                         exclude_from_check=['check_last_ordered_3pc_backup'])
