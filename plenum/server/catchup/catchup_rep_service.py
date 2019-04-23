import math
from collections import defaultdict
from heapq import merge
from random import shuffle
from typing import Optional, List, Tuple, Any

from plenum.common.channel import RxChannel, TxChannel, Router
from plenum.common.constants import CATCH_UP_PREFIX
from plenum.common.ledger import Ledger
from plenum.common.messages.node_messages import ConsistencyProof, CatchupRep, CatchupReq
from plenum.common.metrics_collector import MetricsCollector, MetricsName
from plenum.common.timer import TimerService
from plenum.server.catchup.utils import CatchupDataProvider, LedgerCatchupComplete, CatchupTill
from stp_core.common.log import getlogger

logger = getlogger()


class CatchupRepService:
    def __init__(self,
                 ledger_id: int,
                 config: object,
                 input: RxChannel,
                 output: TxChannel,
                 timer: TimerService,
                 metrics: MetricsCollector,
                 provider: CatchupDataProvider):
        Router(input).add(CatchupRep, self.process_catchup_rep)

        self._ledger_id = ledger_id
        self._ledger = provider.ledger(ledger_id)
        self._config = config
        self._output = output
        self._timer = timer
        self.metrics = metrics
        self._provider = provider
        self._is_working = False
        self._catchup_till = None  # type: Optional[CatchupTill]

        # Nodes are added in this set when the current node sent a CatchupReq
        # for them and waits a CatchupRep message.
        self._wait_catchup_rep_from = set()

        self._received_catchup_replies_from = defaultdict(list)  # type: Dict[int, List]
        self._received_catchup_txns = []  # type: List[Tuple[int, Any]]

    def __repr__(self):
        return "{}:CatchupRepService:{}".format(self._provider.node_name(), self._ledger_id)

    def is_working(self) -> bool:
        return self._is_working

    def start(self, catchup_till: Optional[CatchupTill]):
        logger.info("{} started catching up till {}".format(self, catchup_till))

        self._is_working = True
        self._catchup_till = catchup_till

        if catchup_till is None:
            self._finish()
            return

        if self._ledger.size >= self._catchup_till.final_size:
            logger.info('{} found that ledger {} does not need catchup'.format(self, self._ledger_id))
            self._finish()
            return

        eligible_nodes = self._provider.eligible_nodes()
        if len(eligible_nodes) == 0:
            logger.info('{}{} needs to catchup ledger {} but it has not'
                        ' found any connected nodes'.format(CATCH_UP_PREFIX, self, self._ledger_id))
            return

        reqs = self._gen_catchup_reqs(catchup_till)
        if len(reqs) == 0:
            return

        for (req, to) in zip(reqs, eligible_nodes):
            self._send_catchup_req(req, to)

        timeout = self._catchup_timeout(len(reqs))
        self._timer.schedule(timeout, self._request_txns_if_needed)

    def process_catchup_rep(self, rep: CatchupRep, frm: str):
        if not self._can_process_catchup_rep(rep):
            return

        self._wait_catchup_rep_from.discard(frm)

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

        if self._ledger.size >= self._catchup_till.final_size:
            self._finish()

    def _finish(self, last_3pc: Optional[Tuple[int, int]] = None):
        num_caught_up = self._catchup_till.final_size - self._catchup_till.start_size if self._catchup_till else 0

        self._wait_catchup_rep_from.clear()

        self._is_working = False
        self._received_catchup_txns.clear()
        self._received_catchup_replies_from.clear()
        self._provider.notify_catchup_complete(self._ledger_id)

        logger.info("{}{} completed catching up ledger {}, caught up {} in total"
                    .format(CATCH_UP_PREFIX, self, self._ledger_id, num_caught_up),
                    extra={'cli': True})
        self._output.put_nowait(LedgerCatchupComplete(ledger_id=self._ledger_id,
                                                      num_caught_up=num_caught_up))

    def _gen_catchup_reqs(self, catchup_till: CatchupTill):
        # TODO: This needs to be optimised, there needs to be a minimum size
        # of catchup requests so if a node is trying to catchup only 50 txns
        # from 10 nodes, each of thise 10 nodes will servce 5 txns and prepare
        # a consistency proof for other txns. This is bad for the node catching
        #  up as it involves more network traffic and more computation to verify
        # so many consistency proofs and for the node serving catchup reqs. But
        # if the node sent only 2 catchup requests the network traffic greatly
        # reduces and 25 txns can be read of a single chunk probably
        # (if txns dont span across multiple chunks). A practical value of this
        # "minimum size" is some multiple of chunk size of the ledger
        node_count = len(self._provider.eligible_nodes())
        if node_count == 0:
            logger.info('{} did not find any connected to nodes to send CatchupReq'.format(self))
            return
        # TODO: Consider setting start to `max(ledger.size, consProof.start)`
        # since ordered requests might have been executed after receiving
        # sufficient ConsProof in `preCatchupClbk`
        return self.__gen_catchup_reqs(catchup_till.start_size, catchup_till.final_size, node_count)

    def __gen_catchup_reqs(self, start, end, node_count):
        batch_length = math.ceil((end - start) / node_count)
        reqs = []
        s = start + 1
        e = min(s + batch_length - 1, end)
        for i in range(node_count):
            req = CatchupReq(self._ledger_id, s, e, end)
            reqs.append(req)
            s = e + 1
            e = min(s + batch_length - 1, end)
            if s > end:
                break
        return reqs

    def _catchup_timeout(self, num_requests: int):
        return num_requests * self._config.CatchupTransactionsTimeout

    def _send_catchup_req(self, msg: CatchupReq, to: str):
        self._wait_catchup_rep_from.add(to)
        self._provider.send_to(msg, to)

    def _num_missing_txns(self):
        if self._catchup_till is None:
            return 0
        needed_txns = self._catchup_till.final_size - self._ledger.size
        num_missing = needed_txns - len(self._received_catchup_txns)
        return num_missing if num_missing > 0 else 0

    def _request_txns_if_needed(self):
        if not self._is_working:
            return

        num_missing = self._num_missing_txns()
        if num_missing == 0:
            logger.info('{} not missing any transactions for ledger {}'.format(self, self._ledger_id))
            return

        logger.info("{} requesting {} missing transactions after timeout".format(self, num_missing))
        eligible_nodes = self._provider.eligible_nodes()
        if not self._wait_catchup_rep_from.issuperset(eligible_nodes):
            eligible_nodes = [n for n in eligible_nodes
                              if n not in self._wait_catchup_rep_from]
        self._wait_catchup_rep_from.clear()

        if not eligible_nodes:
            # TODO: What if all nodes are blacklisted so `eligibleNodes`
            # is empty? It will lead to divide by 0. This should not happen
            #  but its happening.
            # https://www.pivotaltracker.com/story/show/130602115
            logger.error("{}{} could not find any node to request "
                         "transactions from. Catchup process cannot "
                         "move ahead.".format(CATCH_UP_PREFIX, self))
            return

        # Shuffling order of nodes so that catchup requests don't go to
        # the same nodes. This is done to avoid scenario where a node
        # does not reply at all.
        # TODO: Need some way to detect nodes that are not responding.
        shuffle(eligible_nodes)
        batchSize = math.ceil(num_missing / len(eligible_nodes))
        cReqs = []
        lastSeenSeqNo = self._ledger.size
        leftMissing = num_missing

        start = self._catchup_till.start_size
        end = self._catchup_till.final_size

        def addReqsForMissing(frm, to):
            # Add Catchup requests for missing transactions.
            # `frm` and `to` are inclusive
            missing = to - frm + 1
            numBatches = int(math.ceil(missing / batchSize))
            for i in range(numBatches):
                s = frm + (i * batchSize)
                e = min(to, frm + ((i + 1) * batchSize) - 1)
                req = CatchupReq(self._ledger_id, s, e, end)
                logger.info("{} creating catchup request {} to {} till {}".format(self, s, e, end))
                cReqs.append(req)
            return missing

        txns = self._received_catchup_txns
        for seqNo, txn in txns:
            if (seqNo - lastSeenSeqNo) != 1:
                missing = addReqsForMissing(lastSeenSeqNo + 1, seqNo - 1)
                leftMissing -= missing
            lastSeenSeqNo = seqNo

        # If still missing some transactions from request has not been
        # sent then either `catchUpReplies` was empty or it did not have
        #  transactions till `end`
        if leftMissing > 0:
            logger.info("{} still missing {} transactions after "
                        "looking at receivedCatchUpReplies".format(self, leftMissing))
            # `catchUpReplies` was empty
            if lastSeenSeqNo == self._ledger.size:
                missing = addReqsForMissing(self._ledger.size + 1, end)
                leftMissing -= missing
            # did not have transactions till `end`
            elif lastSeenSeqNo != end:
                missing = addReqsForMissing(lastSeenSeqNo + 1, end)
                leftMissing -= missing
            else:
                logger.error("{}{} still missing {} transactions. "
                             "Something happened which was not thought "
                             "of. {} {} {}"
                             .format(CATCH_UP_PREFIX, self, leftMissing,
                                     start, end, lastSeenSeqNo))
            if leftMissing:
                logger.error("{}{} still missing {} transactions. {} {} {}"
                             .format(CATCH_UP_PREFIX, self, leftMissing,
                                     start, end, lastSeenSeqNo))

        numElgNodes = len(eligible_nodes)
        for i, req in enumerate(cReqs):
            nodeName = eligible_nodes[i % numElgNodes]
            self._send_catchup_req(req, nodeName)

        timeout = int(self._catchup_timeout(len(cReqs)))
        self._timer.schedule(timeout, self._request_txns_if_needed)

    def _can_process_catchup_rep(self, rep: CatchupRep) -> bool:
        if rep.ledgerId != self._ledger_id:
            return False

        if not self._is_working:
            logger.info('{} ignoring {} since it is not gathering catchup replies'.format(self, rep))
            return False

        return True

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
        final_size = self._catchup_till.final_size
        final_hash = self._catchup_till.final_hash
        try:
            logger.info("{} verifying proof for {}, {}, {}, {}, {}".
                        format(self, temp_tree.tree_size, final_size,
                               temp_tree.root_hash, final_hash, proof))
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
        self._is_working = False
        self._catchup_till = None

        self._wait_catchup_rep_from.clear()
        self._received_catchup_replies_from.clear()
        self._received_catchup_txns.clear()
