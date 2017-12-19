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
    This is a simple policy applying all requests and checking that the result state equals to the expected one
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

        msg = msg.msg
        if not isinstance(msg, dict):
            msg = self._node.toDict(msg)

        if not self._can_process(msg):
            return

        self._add_batch(msg, sender)

        if self._can_apply(msg):
            self._do_apply_data(msg)

        self._process_stashed_messages()

    @staticmethod
    def seq_no(msg):
        return msg[f.SEQ_NO.nm]

    def _can_process(self, msg):
        if not self._last_applied_seq_no:
            return True

        if self._last_applied_seq_no >= self.seq_no(msg):
            logger.debug("{} do not process BATCH with seqNo {} since last applied is {}"
                         .format(OBSERVER_PREFIX, str(self.seq_no(msg)), str(self._last_applied_seq_no)))
            return False

        return True

    def _add_batch(self, msg, sender):
        seq_no = self.seq_no(msg)

        if seq_no not in self._batches:
            self._batches[seq_no] = {}
            heappush(self._batch_seq_no, seq_no)

        self._batches[seq_no][sender] = msg

    def _can_apply(self, msg):
        next_seq_no = self._batch_seq_no[0]
        if self.seq_no(msg) != next_seq_no:
            logger.debug("{} can not apply BATCH with timestamp {} since next expected timestamp is {}"
                         .format(OBSERVER_PREFIX, str(self.seq_no(msg)), str(next_seq_no)))
            return False

        if self.__can_apply_proved(msg):
            return True
        if self.__can_apply_quorumed(msg):
            return True
        return False

    def __can_apply_proved(self, msg):
        # TODO: support write state proofs
        return False

    def __can_apply_quorumed(self, msg):
        quorum = self._node.quorums.observer_data
        batches_for_msg = self._batches[self.seq_no(msg)]  # {sender: msg}
        num_batches = len(batches_for_msg)
        if not quorum.is_reached(len(batches_for_msg)):
            logger.debug("{} can not apply BATCH with timestamp {} since no quorum yet ({} of {})"
                         .format(OBSERVER_PREFIX, str(self.seq_no(msg)), num_batches, str(quorum.value)))
            return False

        msgs = [json.dumps(msg, sort_keys=True)
                for msg in batches_for_msg.values()]
        result, freq = mostCommonElement(msgs)
        if not quorum.is_reached(freq):
            logger.debug(
                "{} can not apply BATCH with timestamp {} since have just {} equal elements ({} needed for quorum)"
                    .format(OBSERVER_PREFIX, str(self.seq_no(msg)), freq, str(quorum.value)))
            return False

        return True

    def _do_apply_data(self, msg):
        logger.info("{} applying BATCH {}"
                    .format(OBSERVER_PREFIX, msg))

        self._do_apply_batch(msg)

        del self._batches[self.seq_no(msg)]
        heappop(self._batch_seq_no)
        self._last_applied_seq_no = self.seq_no(msg)

    def _do_apply_batch(self, batch):
        reqs = [Request(**req_dict) for req_dict in batch[f.REQUEST.nm]]

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

            next_pp_time = self._batch_seq_no[0]
            next_msg = next(iter(
                self._batches[next_pp_time].values()))

            if not self._can_apply(next_msg):
                break

            self._do_apply_data(next_msg)
