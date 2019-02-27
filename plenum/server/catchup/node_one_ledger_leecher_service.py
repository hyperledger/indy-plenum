from collections import defaultdict
from enum import Enum
from heapq import merge
from typing import Optional, List, Tuple, Any, NamedTuple

from plenum.common.channel import RxChannel, TxChannel
from plenum.common.ledger import Ledger
from plenum.common.messages.node_messages import LedgerStatus, ConsistencyProof, CatchupRep
from plenum.common.metrics_collector import MetricsCollector, MetricsName
from plenum.server.catchup.utils import CatchupDataProvider
from stp_core.common.log import getlogger

logger = getlogger()


class NodeOneLedgerLeecherService:
    def __init__(self,
                 ledger_id: int,
                 input: RxChannel,
                 output: TxChannel,
                 metrics: MetricsCollector,
                 provider: CatchupDataProvider):
        input.set_handler(CatchupRep, self.process_catchup_rep)

        self._ledger_id = ledger_id
        self._ledger = provider.ledger(ledger_id)
        self._output = output
        self.metrics = metrics
        self._provider = provider
        self._is_working = False
        self._catchup_till = None  # type: Optional[ConsistencyProof]

        # Nodes are added in this set when the current node sent a CatchupReq
        # for them and waits a CatchupRep message.
        self._wait_catchup_rep_from = set()

        self._received_catchup_replies_from = defaultdict(list)  # type: Dict[int, List]
        self._received_catchup_txns = []  # type: List[Tuple[int, Any]]

    def is_working(self) -> bool:
        return self._is_working

    def start(self, cons_proof: ConsistencyProof):
        self._catchup_till = cons_proof
        # self._gather_catchup_replies()

    def process_catchup_rep(self, rep: CatchupRep, frm: str):
        logger.info("{} received catchup reply from {}: {}".format(self, frm, rep))
        self._wait_catchup_rep_from.discard(frm)

        if not self._can_process_catchup_rep(rep):
            return

        txns = self._get_interesting_txns_from_catchup_rep(rep)
        if len(txns) == 0:
            return

        logger.info("{} found {} interesting transactions in the catchup from {}".format(self, len(txns), frm))
        self.metrics.add_event(MetricsName.CATCHUP_TXNS_RECEIVED, len(txns))

        self._received_catchup_replies_from[frm].append(rep)

        txns_already_rcvd_in_catchup = self._merge_catchup_txns(self._received_catchup_txns, txns)
        logger.info("{} merged catchups, there are {} of them now, from {} to {}".
                    format(self, len(txns_already_rcvd_in_catchup), txns_already_rcvd_in_catchup[0][0],
                           txns_already_rcvd_in_catchup[-1][0]))

        num_processed = self._process_catchup_txns(txns_already_rcvd_in_catchup)
        logger.info("{} processed {} catchup replies with sequence numbers {}".
                    format(self, num_processed,
                           [seq_no for seq_no, _ in txns_already_rcvd_in_catchup[:num_processed]]))

        self._received_catchup_txns = txns_already_rcvd_in_catchup[num_processed:]

        if self._ledger.size >= self._catchup_till.seqNoEnd:
            # TODO: self._output.put_nowait(OneLedgerCatchupFinished(self._ledger_id))
            self._reset()

    def _can_process_catchup_rep(self, rep: CatchupRep) -> bool:
        if not self._is_working:
            logger.info('{} ignoring {} since it is not gathering catchup replies'.format(self, rep))
            return False

        if rep.ledgerId != self._ledger_id:
            logger.warning('{} cannot process {} for different ledger'.format(self, rep))
            return False

    def _get_interesting_txns_from_catchup_rep(self, rep: CatchupRep) -> List[Tuple[int, Any]]:
        ledger = self._provider.ledger(self._ledger_id)
        txns = ((int(s), t) for s, t in rep.txns.items())
        txns = sorted(txns, key=lambda v: v[0])

        if not any(s > ledger.size for s, _ in txns):
            self._provider.discard(rep,
                                   reason="ledger has size {} and it already contains all transactions in the reply".
                                   format(ledger.size), logMethod=logger.info)
            return []

        if not all(next[0] == prev[0] + 1 for prev, next in zip(txns, txns[1:])):
            self._provider.discard(rep, reason="contains duplicates or gaps", logMethod=logger.info)
            return []

        return txns

    @staticmethod
    def _merge_catchup_txns(existing_txns, new_txns):
        """
        Merge any newly received txns during catchup with already received txns
        :param existing_txns:
        :param new_txns:
        :return:
        """
        # TODO: Can we replace this with SortedDict and before merging substract existing transactions from new?
        idx_to_remove = []
        start_seq_no = new_txns[0][0]
        end_seq_no = new_txns[-1][0]
        for seq_no, _ in existing_txns:
            if seq_no < start_seq_no:
                continue
            if seq_no > end_seq_no:
                break
            idx_to_remove.append(seq_no - start_seq_no)
        for idx in reversed(idx_to_remove):
            new_txns.pop(idx)

        return list(merge(existing_txns, new_txns, key=lambda v: v[0]))

    def _process_catchup_txns(self, txns: List[Tuple[int, Any]]) -> int:
        # Removing transactions for sequence numbers are already
        # present in the ledger
        # TODO: Inefficient, should check list in reverse and stop at first
        # match since list is already sorted
        num_processed = sum(1 for s, _ in txns if s <= self._ledger.size)
        if num_processed:
            logger.info("{} found {} already processed transactions in the catchup replies".
                        format(self, num_processed))

        # If `catchUpReplies` has any transaction that has not been applied
        # to the ledger
        txns = txns[num_processed:]
        while txns and txns[0][0] - self._ledger.seqNo == 1:
            seq_no = txns[0][0]
            result, node_name, to_be_processed = self._has_valid_catchup_replies(seq_no, txns)
            if result:
                for _, txn in txns[:to_be_processed]:
                    self._add_txn(txn)
                self._remove_processed_catchup_reply(node_name, seq_no)
                num_processed += to_be_processed
                txns = txns[to_be_processed:]
            else:
                self._provider.blacklist_node(
                    node_name,
                    reason="Sent transactions that could not be verified")
                self._remove_processed_catchup_reply(node_name, seq_no)
                # Invalid transactions have to be discarded so letting
                # the caller know how many txns have to removed from
                # `self.receivedCatchUpReplies`
                return num_processed + to_be_processed

        return num_processed

    def _has_valid_catchup_replies(self, seq_no: int, txns_to_process: List[Tuple[int, Any]]) -> Tuple[bool, str, int]:
        """
        Transforms transactions for ledger!

        Returns:
            Whether catchup reply corresponding to seq_no
            Name of node from which txns came
            Number of transactions ready to be processed
        """

        # TODO: Remove after stop passing seqNo here
        assert seq_no == txns_to_process[0][0]

        # Here seqNo has to be the seqNo of first transaction of
        # `catchupReplies`

        # Get the transactions in the catchup reply which has sequence
        # number `seqNo`
        node_name, catchup_rep = self._find_catchup_reply_for_seq_no(seq_no)
        txns = catchup_rep.txns

        # Add only those transaction in the temporary tree from the above
        # batch which are not present in the ledger
        # Integer keys being converted to strings when marshaled to JSON
        txns = [self._provider.transform_txn_for_ledger(txn)
                for s, txn in txns_to_process[:len(txns)]
                if str(s) in txns]

        # Creating a temporary tree which will be used to verify consistency
        # proof, by inserting transactions. Duplicating a merkle tree is not
        # expensive since we are using a compact merkle tree.
        temp_tree = self._ledger.treeWithAppliedTxns(txns)

        proof = catchup_rep.consProof
        final_size = self._catchup_till.seqNoEnd
        final_hash = self._catchup_till.newMerkleRoot
        try:
            logger.info("{} verifying proof for {}, {}, {}, {}, {}".
                        format(self, temp_tree.tree_size, final_size,
                               temp_tree.root_hash, Ledger.strToHash(final_hash),
                               [Ledger.strToHash(p) for p in proof]))
            verified = self._provider.verifier(self._ledger_id).verify_tree_consistency(
                temp_tree.tree_size,
                final_size,
                temp_tree.root_hash,
                Ledger.strToHash(final_hash),
                [Ledger.strToHash(p) for p in proof]
            )

        except Exception as ex:
            logger.info("{} could not verify catchup reply {} since {}".format(self, catchup_rep, ex))
            verified = False
        return bool(verified), node_name, len(txns)

    def _find_catchup_reply_for_seq_no(self, seq_no: int) -> Tuple[str, CatchupRep]:
        # This is inefficient if we have large number of nodes but since
        # number of node are always between 60-120, this is ok.
        for frm, reps in self._received_catchup_replies_from.items():
            for rep in reps:
                if str(seq_no) in rep.txns:
                    return frm, rep

    def _add_txn(self, txn):
        self._ledger.add(self._provider.transform_txn_for_ledger(txn))
        self._provider.notify_transaction_added_to_ledger(self._ledger_id, txn)

    def _remove_processed_catchup_reply(self, node: str, seq_no: str):
        for i, rep in enumerate(self._received_catchup_replies_from[node]):
            if str(seq_no) in rep.txns:
                break
        self._received_catchup_replies_from[node].pop(i)

    def _reset(self):
        self._state = self.State.Ready
        self._catchup_till = None

        self._wait_catchup_rep_from.clear()
        self._received_catchup_replies_from.clear()
        self._received_catchup_txns.clear()
