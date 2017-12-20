import json
from heapq import heappush, heappop
from logging import getLogger

from plenum.common.constants import BATCH, OBSERVER_PREFIX
from plenum.common.request import Request
from plenum.common.types import f
from plenum.common.util import mostCommonElement
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicy

logger = getLogger()


class ObserverSyncPolicyEachBatch(ObserverSyncPolicy):
    '''
    This is the simplest policy applying applying all committed batches by Validators to Observer Node.
    - It understands BATCH ObservedData type
    - It applies batches according to their seqNo
    - Already processed batches are not applied
    - Before applying each batch, either a state proof is checked (TBD), or we're waiting for the consensus
    (f+1 equal Batches from different validators)
    '''

    def __init__(self, node) -> None:
        super().__init__()
        self._node = node
        self._last_applied_seq_no = None
        self._batches = {}
        self._batch_seq_no = []

    @property
    def policy_type(self) -> str:
        return BATCH

    def apply_data(self, msg, sender):
        assert msg.msg_type == BATCH, \
            "'Each Batch' policy is used for Observer Data message of type {}".format(msg.msg_type)

        logger.debug("{} got BATCH {} from Observable Node {}"
                     .format(OBSERVER_PREFIX, msg, sender))

        # 1. make sure that the message is dict
        batch = msg.msg
        if not isinstance(batch, dict):
            batch = self._node.toDict(batch)

        # 2. check whether we can process the message (that it's not already processed and is the next expected seq_no)
        if not self._can_process(batch):
            return

        # 3. stash the batch
        self._add_batch(batch, sender)

        # 4. check that the batch can be applied
        # - this is the next expected batch (according to seq_nos)
        # - either state proof is valid, or a consensus of f+1 equal batches from different nodes
        if not self._can_apply(batch):
            return

        # 5. apply the batch (write to ledger and state)
        self._do_apply_data(batch)

        # 6. check whether we can apply stashed batches with next seq_no
        self._process_stashed_messages()

    @staticmethod
    def seq_no_start(batch):
        return batch[f.SEQ_NO_START.nm]

    @staticmethod
    def seq_no_end(batch):
        return batch[f.SEQ_NO_END.nm]

    def _can_process(self, batch):
        if not self._last_applied_seq_no:
            return True

        if self._last_applied_seq_no >= self.seq_no_start(batch):
            logger.debug("{} do not process BATCH with seqNo {} since it is already applied (last applied is {})"
                         .format(OBSERVER_PREFIX, str(self.seq_no_start(batch)), str(self._last_applied_seq_no)))
            return False

        return True

    def _add_batch(self, batch, sender):
        seq_no = self.seq_no_start(batch)

        if seq_no not in self._batches:
            self._batches[seq_no] = {}
            heappush(self._batch_seq_no, seq_no)

        self._batches[seq_no][sender] = batch

    def _can_apply(self, batch):
        next_seq_no = self._last_applied_seq_no + 1 if self._last_applied_seq_no else self._batch_seq_no[0]
        if self.seq_no_start(batch) != next_seq_no:
            logger.debug("{} can not apply BATCH with seq_no {} since next expected seq_no is {}"
                         .format(OBSERVER_PREFIX, str(self.seq_no_start(batch)), str(next_seq_no)))
            return False

        if self.__can_apply_proved(batch):
            return True
        if self.__can_apply_quorumed(batch):
            return True
        return False

    def __can_apply_proved(self, batch):
        # TODO: support write state proofs
        return False

    def __can_apply_quorumed(self, batch):
        quorum = self._node.quorums.observer_data
        batches_for_msg = self._batches[self.seq_no_start(batch)]  # {sender: msg}
        num_batches = len(batches_for_msg)
        if not quorum.is_reached(len(batches_for_msg)):
            logger.debug("{} can not apply BATCH with seq_no {} since no quorum yet ({} of {})"
                         .format(OBSERVER_PREFIX, str(self.seq_no_start(batch)), num_batches, str(quorum.value)))
            return False

        msgs = [json.dumps(msg, sort_keys=True)
                for msg in batches_for_msg.values()]
        result, freq = mostCommonElement(msgs)
        if not quorum.is_reached(freq):
            logger.debug(
                "{} can not apply BATCH with seq_no {} since have just {} equal elements ({} needed for quorum)"
                .format(OBSERVER_PREFIX, str(self.seq_no_start(batch)), freq, str(quorum.value)))
            return False

        return True

    def _do_apply_data(self, batch):
        logger.info("{} applying BATCH {}"
                    .format(OBSERVER_PREFIX, batch))

        self._do_apply_batch(batch)

        del self._batches[self.seq_no_start(batch)]
        heappop(self._batch_seq_no)
        self._last_applied_seq_no = self.seq_no_end(batch)

    def _do_apply_batch(self, batch):
        reqs = [Request(**req_dict) for req_dict in batch[f.REQUESTS.nm]]

        pp_time = batch[f.PP_TIME.nm]
        ledger_id = batch[f.LEDGER_ID.nm]
        state_root = batch[f.STATE_ROOT.nm]
        txn_root = batch[f.TXN_ROOT.nm]

        self._node.apply_reqs(reqs,
                              pp_time,
                              ledger_id)
        self._node.get_executer(ledger_id)(pp_time,
                                           reqs,
                                           state_root,
                                           txn_root)

    def _process_stashed_messages(self):
        while True:
            if not self._batches:
                break
            if not self._batch_seq_no:
                break

            next_seq_no = self._batch_seq_no[0]
            next_batch = next(iter(
                self._batches[next_seq_no].values()))

            if not self._can_apply(next_batch):
                break

            self._do_apply_data(next_batch)
