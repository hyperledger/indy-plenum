from collections import Counter
from typing import Optional, NamedTuple

from ledger.ledger import Ledger
from plenum.common.channel import RxChannel, TxChannel, Router
from plenum.common.constants import LEDGER_STATUS, CONSISTENCY_PROOF
from plenum.common.messages.node_messages import LedgerStatus, MessageReq, ConsistencyProof
from plenum.common.metrics_collector import MetricsCollector, measure_time, MetricsName
from plenum.common.timer import TimerService
from plenum.common.types import f
from plenum.common.util import min_3PC_key
from plenum.server.catchup.utils import CatchupDataProvider, build_ledger_status, LedgerCatchupStart, CatchupTill
from plenum.server.quorums import Quorums
from stp_core.common.log import getlogger

logger = getlogger()


# ASSUMING NO MALICIOUS NODES
# Assuming that all nodes have the same state of the system and no node
# is lagging behind. So if two new nodes are added in quick succession in a
# high traffic environment, this logic is faulty

class ConsProofService:
    def __init__(self,
                 ledger_id: int,
                 config: object,
                 input: RxChannel,
                 output: TxChannel,
                 timer: TimerService,
                 metrics: MetricsCollector,
                 provider: CatchupDataProvider):
        router = Router(input)
        router.add(LedgerStatus, self.process_ledger_status)
        router.add(ConsistencyProof, self.process_consistency_proof)

        self._ledger_id = ledger_id
        self._ledger = provider.ledger(ledger_id)
        self._config = config
        self._output = output
        self._timer = timer
        self.metrics = metrics
        self._provider = provider
        self._is_working = False

        self._quorum = Quorums(len(self._provider.all_nodes_names()))
        self._same_ledger_status = set()
        self._cons_proofs = {}
        self._requested_consistency_proof = set()
        self._last_txn_3PC_key = {}

    def __repr__(self) -> str:
        return "{}:ConsProofService:{}".format(self._provider.node_name(), self._ledger_id)

    def start(self, request_ledger_statuses: bool):
        logger.info("{} starts".format(self))

        self._is_working = True
        self._quorum = Quorums(len(self._provider.all_nodes_names()))
        self._same_ledger_status = set()
        self._cons_proofs = {}
        self._requested_consistency_proof = set()
        self._last_txn_3PC_key = {}

        if request_ledger_statuses:
            self._request_ledger_status_from_nodes()
            self._schedule_reask_ledger_status()

    def process_ledger_status(self, ledger_status: LedgerStatus, frm: str):
        if not self._can_process_ledger_status(ledger_status):
            return

        if self._is_ledger_old(ledger_status):
            self._process_newer_ledger_status(ledger_status, frm)
        else:
            self._process_same_ledger_status(ledger_status, frm)

    @measure_time(MetricsName.PROCESS_CONSISTENCY_PROOF_TIME)
    def process_consistency_proof(self, proof: ConsistencyProof, frm: str):
        if not self._can_process_consistency_proof(proof):
            return

        logger.info("{} received consistency proof: {} from {}".format(self, proof, frm))
        self._cons_proofs[frm] = ConsistencyProof(*proof)

        if not self._is_catchup_needed():
            self._finish_no_catchup()
            return

        if self._should_schedule_reask_cons_proofs():
            self._schedule_reask_cons_proof()

        cp = self._get_cons_proof_for_catchup()
        if not cp:
            return

        self._finish(cp)

    def _finish(self, cons_proof: Optional[ConsistencyProof] = None):
        logger.info("{} finished with consistency proof {}".format(self, cons_proof))

        till = CatchupTill(start_size=cons_proof.seqNoStart,
                           final_size=cons_proof.seqNoEnd,
                           final_hash=cons_proof.newMerkleRoot) if cons_proof else None

        nodes_ledger_sizes = {frm: proof.seqNoEnd
                              for frm, proof in self._cons_proofs.items()
                              if proof is not None}

        # Stop requesting last consistency proofs and ledger statuses.
        self._is_working = False
        self._same_ledger_status = set()
        self._cons_proofs = {}
        self._requested_consistency_proof = set()

        self._cancel_reask()

        self._output.put_nowait(LedgerCatchupStart(ledger_id=self._ledger_id,
                                                   catchup_till=till,
                                                   nodes_ledger_sizes=nodes_ledger_sizes))

    def _finish_no_catchup(self):
        root = Ledger.hashToStr(self._ledger.tree.root_hash)
        last_3pc = self._get_last_txn_3PC_key()
        if not last_3pc:
            self._finish()
            return

        view_no, pp_seq_no = last_3pc
        cons_proof = ConsistencyProof(self._ledger_id,
                                      self._ledger.size,
                                      self._ledger.size,
                                      view_no,
                                      pp_seq_no,
                                      root,
                                      root,
                                      [])
        self._finish(cons_proof)

    def _get_last_txn_3PC_key(self):
        quorumed_3PC_keys = \
            [
                most_common_element
                for most_common_element, freq in
                Counter(self._last_txn_3PC_key.values()).most_common()
                if
                self._quorum.ledger_status_last_3PC.is_reached(
                    freq) and most_common_element[0] is not None and most_common_element[1] is not None
            ]

        if len(quorumed_3PC_keys) == 0:
            return None

        min_quorumed_3PC_key = min_3PC_key(quorumed_3PC_keys)
        return min_quorumed_3PC_key

    @measure_time(MetricsName.SEND_MESSAGE_REQ_TIME)
    def _request_ledger_status_from_nodes(self, nodes=None):
        logger.info("{} asking for ledger status of ledger {}".format(self, self._ledger_id))
        ledger_status_req = MessageReq(
            LEDGER_STATUS,
            {f.LEDGER_ID.nm: self._ledger_id}
        )
        self._provider.send_to_nodes(ledger_status_req, nodes=nodes)

    def _can_process_ledger_status(self, ledger_status: LedgerStatus):
        if ledger_status.ledgerId != self._ledger_id:
            return False

        if not self._is_working:
            logger.info('{} ignoring {} since it is not gathering ledger statuses'.format(self, ledger_status))
            return False

        return True

    def _can_process_consistency_proof(self, proof: ConsistencyProof):
        if proof.ledgerId != self._ledger_id:
            return False

        if not self._is_working:
            logger.info('{} ignoring {} since it is not gathering consistency proofs'.format(self, proof))
            return False

        start = getattr(proof, f.SEQ_NO_START.nm)
        end = getattr(proof, f.SEQ_NO_END.nm)

        # TODO: Should we discard where start is older than the ledger size
        ledger_size = self._ledger.size
        if start > ledger_size:
            self._provider.discard(proof, reason="Start {} is greater than "
                                                 "ledger size {}".
                                   format(start, ledger_size),
                                   logMethod=logger.warning)
            return False

        if end <= start:
            self._provider.discard(proof, reason="End {} is not greater than "
                                                 "start {}".format(end, start),
                                   logMethod=logger.warning)
            return False

        return True

    def _process_newer_ledger_status(self, ledger_status: LedgerStatus, frm: str):
        # If we are behind the node which has sent the ledger status
        # then send our ledger status to it
        # in order to get the consistency proof from it
        my_ledger_status = build_ledger_status(self._ledger_id, self._provider)
        self._provider.send_to(my_ledger_status, frm)
        self._schedule_reask_last_cons_proof(frm)

    def _process_same_ledger_status(self, ledger_status: LedgerStatus, frm: str):
        # We are not behind the node which has sent the ledger status,
        # so our ledger is OK in comparison with that node
        # and we will not get a consistency proof from it
        self._same_ledger_status.add(frm)
        self._cons_proofs[frm] = None

        # If we are even with the node which has sent the ledger status
        # then save the last txn master 3PC-key from it
        if self._is_ledger_same(ledger_status):
            self._last_txn_3PC_key[frm] = (ledger_status.viewNo, ledger_status.ppSeqNo)

        if not self._is_catchup_needed():
            self._finish_no_catchup()

    def _is_ledger_old(self, status: LedgerStatus):
        # Is self ledger older than the `LedgerStatus`
        return self._compare_ledger(status) < 0

    def _is_ledger_same(self, status: LedgerStatus):
        # Is self ledger same as the `LedgerStatus`
        return self._compare_ledger(status) == 0

    def _compare_ledger(self, status: LedgerStatus):
        ledgerId = getattr(status, f.LEDGER_ID.nm)
        seqNo = getattr(status, f.TXN_SEQ_NO.nm)
        ledger = self._provider.ledger(self._ledger_id)
        logger.info("{} comparing its ledger {} of size {} with {}".
                    format(self, ledgerId, ledger.seqNo, seqNo))
        return ledger.seqNo - seqNo

    def _is_catchup_needed(self):
        # If we gathered the quorum of ledger statuses indicating
        # that our ledger is OK then we do not need to perform actual
        # synchronization of our ledger (however, we still need to do
        # the ledger pre-catchup and post-catchup procedures)
        if self._quorum.ledger_status.is_reached(len(self._same_ledger_status)):
            return False

        # If at least n-f-1 nodes were found to be at the same state
        # then this node's state is good too
        if not self._quorum.consistency_proof.is_reached(len(self._cons_proofs)):
            return True

        grpd_prf, null_proofs_count = self._get_group_consistency_proofs(self._cons_proofs)
        if self._quorum.ledger_status.is_reached(null_proofs_count):
            return False

        return True

    def _should_schedule_reask_cons_proofs(self):
        return len([v for v in self._cons_proofs.values() if v is not None]) == self._quorum.f + 1

    def _get_cons_proof_for_catchup(self):
        if not self._quorum.consistency_proof.is_reached(len(self._cons_proofs)):
            return None

        grpd_prf, _ = self._get_group_consistency_proofs(self._cons_proofs)
        result = self._get_latest_reliable_proof(grpd_prf)
        if not result:
            logger.info("{} cannot start catchup since received only {} "
                        "consistency proofs but need at least {}".
                        format(self, len(self._cons_proofs), self._quorum.consistency_proof.value))
            return None

        return ConsistencyProof(self._ledger_id, *result)

    def _get_group_consistency_proofs(self, proofs):
        logger.info("{} deciding on the basis of CPs {} and f {}".
                    format(self, proofs, self._quorum.f))

        recvdPrf = {}
        # For the case where the other node is at the same state as
        # this node
        nullProofs = 0
        for nodeName, proof in proofs.items():
            if proof:
                start, end = getattr(proof, f.SEQ_NO_START.nm), getattr(proof, f.SEQ_NO_END.nm)
                if (start, end) not in recvdPrf:
                    recvdPrf[(start, end)] = {}
                key = (
                    getattr(proof, f.VIEW_NO.nm),
                    getattr(proof, f.PP_SEQ_NO.nm),
                    getattr(proof, f.OLD_MERKLE_ROOT.nm),
                    getattr(proof, f.NEW_MERKLE_ROOT.nm),
                    tuple(getattr(proof, f.HASHES.nm))
                )
                recvdPrf[(start, end)][key] = recvdPrf[(start, end)].get(key, 0) + 1
            else:
                logger.info("{} found proof by {} null".format(self, nodeName))
                nullProofs += 1
        return recvdPrf, nullProofs

    def _get_reliable_proofs(self, grouped_proofs):
        result = {}
        for (start, end), val in grouped_proofs.items():
            for (view_no, lastPpSeqNo, oldRoot,
                 newRoot, hashes), count in val.items():
                if self._quorum.same_consistency_proof.is_reached(count):
                    result[(start, end)] = (view_no, lastPpSeqNo, oldRoot,
                                            newRoot, hashes)
                    # There would be only one correct proof for a range of
                    # sequence numbers
                    break
        return result

    def _get_latest_reliable_proof(self, grouped_proofs):
        reliableProofs = self._get_reliable_proofs(grouped_proofs)
        latest = None
        for (start, end), (view_no, last_pp_seq_no, oldRoot,
                           newRoot, hashes) in reliableProofs.items():
            # TODO: Can we do something where consistency proof's start is older
            #  than the current ledger's size and proof's end is larger
            # than the current ledger size.
            # Ignore if proof's start is not the same as the ledger's end
            if start != self._ledger.size:
                continue
            if latest is None or latest[1] < end:
                latest = (start, end) + (view_no, last_pp_seq_no,
                                         oldRoot, newRoot, hashes)
        return latest

    def _reask_for_ledger_status(self):
        nodes = [node_name for node_name in self._provider.all_nodes_names() if
                 node_name not in self._same_ledger_status and
                 node_name != self._provider.node_name()]
        self._request_ledger_status_from_nodes(nodes)

    def _reask_for_last_consistency_proof(self):
        ledger_status = build_ledger_status(self._ledger_id, self._provider)
        nodes = [node_name for node_name in self._provider.all_nodes_names() if
                 node_name not in self._cons_proofs and
                 node_name != self._provider.node_name()]
        self._provider.send_to_nodes(ledger_status, nodes=nodes)

    def _request_CPs_if_needed(self):
        if not self._is_working:
            return

        proofs = self._cons_proofs
        # there is no any received ConsistencyProofs
        if not proofs:
            return

        logger.info("{} requesting consistency proofs after timeout".format(self))

        if not self._is_catchup_needed():
            return

        grouped_proofs, null_proofs_count = self._get_group_consistency_proofs(proofs)
        if self._quorum.ledger_status.is_reached(null_proofs_count) \
                or len(grouped_proofs) == 0:
            return

        if self._get_latest_reliable_proof(grouped_proofs):
            return

        with self.metrics.measure_time(MetricsName.SEND_MESSAGE_REQ_TIME):
            start, end = self._get_consistency_proof_request_params(grouped_proofs)
            logger.info("{} sending consistency proof request: {}".format(self, self._ledger_id, start, end))
            cons_proof_req = MessageReq(
                CONSISTENCY_PROOF,
                {f.LEDGER_ID.nm: self._ledger_id,
                 f.SEQ_NO_START.nm: start,
                 f.SEQ_NO_END.nm: end}
            )
            self._provider.send_to_nodes(cons_proof_req, nodes=self._provider.eligible_nodes())

    def _get_consistency_proof_request_params(self, groupedProofs):
        # Choose the consistency proof which occurs median number of times in
        # grouped proofs. Not choosing the highest since some malicious nodes
        # might be sending non-existent sequence numbers and not choosing the
        # lowest since that might not be enough as some nodes must be lagging
        # behind a lot or some malicious nodes might send low sequence numbers.
        proofs = sorted(groupedProofs.items(),
                        key=lambda t: max(t[1].values()))
        return self._ledger.size, proofs[len(proofs) // 2][0][1]

    def _schedule_reask_cons_proof(self):
        # At least once correct node believes that this node is behind.

        # Stop requesting last consistency proofs and ledger statuses.
        self._cancel_reask()

        # Start timer that will expire in some time and if till that time
        # enough CPs are not received, then explicitly request CPs
        # from other nodes, see `request_CPs_if_needed`
        self._timer.schedule(
            delay=self._config.ConsistencyProofsTimeout * (len(self._provider.all_nodes_names()) - 1),
            callback=self._request_CPs_if_needed
        )

    def _schedule_reask_ledger_status(self):
        self._timer.schedule(
            delay=self._config.LedgerStatusTimeout * (len(self._provider.all_nodes_names()) - 1),
            callback=self._reask_for_ledger_status
        )

    def _schedule_reask_last_cons_proof(self, frm):
        if frm not in self._requested_consistency_proof:
            self._requested_consistency_proof.add(frm)
            self._timer.schedule(
                delay=self._config.ConsistencyProofsTimeout * (len(self._provider.all_nodes_names()) - 1),
                callback=self._reask_for_last_consistency_proof
            )

    def _cancel_reask(self):
        self._timer.cancel(self._reask_for_last_consistency_proof)
        self._timer.cancel(self._reask_for_ledger_status)
        self._timer.cancel(self._request_CPs_if_needed)
