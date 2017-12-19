from collections import deque
from logging import getLogger

from plenum.common.constants import BATCH, OBSERVER_PREFIX
from plenum.common.request import Request
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
        self._last_applied_ts = None
        self._batches = {}
        self._batch_ts = deque()

    @property
    def policy_type(self) -> str:
        return BATCH

    def apply_data(self, msg, sender):
        assert msg.msg_type == BATCH, \
            "'Each Batch' policy is used for Observer Data message of type {}".format(msg.msg_type)

        logger.debug("{} got BATCH {} from Observable Node {}"
                     .format(OBSERVER_PREFIX, msg, sender))

        if not self._can_process(msg):
            return

        self._add_batch(msg, sender)

        if self._can_apply(msg):
            self._do_apply_data(msg)

    @staticmethod
    def pp_time(msg):
        return msg.msg.ppTime

    def _can_process(self, msg):
        if not self._last_applied_ts:
            return True

        if self._last_applied_ts >= self.pp_time(msg):
            logger.debug("{} do not process BATCH with timestamp {} since last applied is {}"
                         .format(OBSERVER_PREFIX, str(self.pp_time(msg)), str(self._last_applied_ts)))
            return False

        return True

    def _add_batch(self, msg, sender):
        pp_time = self.pp_time(msg)

        if pp_time not in self._batches:
            self._batches[pp_time] = {}
            self._batch_ts.append(pp_time)

        self._batches[pp_time][sender] = msg

    def _can_apply(self, msg):
        if self.pp_time(msg) != self._batch_ts[0]:
            logger.debug("{} can not apply BATCH with timestamp {} since next expected timestamp is {}"
                         .format(OBSERVER_PREFIX, str(self.pp_time(msg)), str(self._batch_ts[0])))
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
        batches_for_msg = self._batches[self.pp_time(msg)]  # {sender: msg}
        num_batches = len(batches_for_msg)
        if not quorum.is_reached(len(batches_for_msg)):
            logger.debug("{} can not apply BATCH with timestamp {} since no quorum yet ({} of {})"
                         .format(OBSERVER_PREFIX, str(self.pp_time(msg)), num_batches, str(quorum.value)))
            return False

        result, freq = mostCommonElement(batches_for_msg.values())
        if not quorum.is_reached(freq):
            logger.debug(
                "{} can not apply BATCH with timestamp {} since have just {} equal elements ({} needed for quorum)"
                    .format(OBSERVER_PREFIX, str(self.pp_time(msg)), freq, str(quorum.value)))
            return False

        return True

    def _do_apply_data(self, msg):
        logger.info("{} applying BATCH {}"
                    .format(OBSERVER_PREFIX, msg))

        self._do_apply_batch(msg.msg)

        del self._batches[self.pp_time(msg)]
        self._batch_ts.popleft()
        self._last_applied_ts = self.pp_time(msg)

    def _do_apply_batch(self, batch):
        reqs = [Request(**req_dict) for req_dict in batch.request]

        self._node.apply_reqs(reqs,
                              batch.ppTime,
                              batch.ledgerId)
        self._node.get_executer(batch.ledgerId)(batch.ppTime,
                                                reqs,
                                                batch.stateRootHash,
                                                batch.txnRootHash)
