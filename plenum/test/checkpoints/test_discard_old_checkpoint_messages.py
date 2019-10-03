from plenum.common.constants import AUDIT_LEDGER_ID
from plenum.common.messages.node_messages import Checkpoint
from plenum.server.replica_validator_enums import ALREADY_STABLE
from plenum.test.checkpoints.helper import check_for_instance, check_stable_checkpoint
from stp_core.loop.eventually import eventually
from plenum.test.helper import checkDiscardMsg
from plenum.test.helper import sdk_send_random_and_check


def test_discard_checkpoint_msg_for_stable_checkpoint(chkFreqPatched,
                                                      tconf, looper,
                                                      txnPoolNodeSet,
                                                      sdk_pool_handle,
                                                      sdk_wallet_client,
                                                      reqs_for_checkpoint):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, reqs_for_checkpoint)
    next_checkpoint = tconf.CHK_FREQ
    for inst_id in txnPoolNodeSet[0].replicas.keys():
        looper.run(eventually(check_for_instance, txnPoolNodeSet, inst_id,
                              check_stable_checkpoint, next_checkpoint, retryWait=1))
    node1 = txnPoolNodeSet[0]
    rep1 = node1.replicas[0]
    # TODO: Use old checkpoint message when we retain stabilized checkpoint
    # oldChkpointMsg = rep1._consensus_data.checkpoints[0]
    oldChkpointMsg = Checkpoint(instId=0,
                                viewNo=0,
                                seqNoStart=0,
                                seqNoEnd=next_checkpoint,
                                digest=node1.db_manager.get_txn_root_hash(AUDIT_LEDGER_ID))
    rep1.send(oldChkpointMsg)
    recvReplicas = [n.replicas[0].stasher for n in txnPoolNodeSet[1:]]
    looper.run(eventually(checkDiscardMsg, recvReplicas, oldChkpointMsg,
                          ALREADY_STABLE, retryWait=1))
