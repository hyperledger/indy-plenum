from plenum.common.constants import BATCH
from plenum.common.request import Request
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicy


class ObserverSyncPolicyEachBatch(ObserverSyncPolicy):
    '''
    This is a simple policy applying all requests and checking that the result state equals to the expected one
    '''

    def __init__(self, node) -> None:
        super().__init__()
        self._node = node

    @property
    def policy_type(self) -> str:
        return BATCH

    def apply_data(self, msg):
        assert msg.msg_type == BATCH, \
            "'Each Batch' policy is used for Observer Data message of type {}".format(msg.msg_type)

        batch = msg.msg
        reqs = [Request(**req_dict) for req_dict in batch.request]

        self._node.apply_reqs(reqs,
                              batch.ppTime,
                              batch.ledgerId)
        self._node.get_executer(batch.ledgerId)(batch.ppTime,
                                                reqs,
                                                batch.stateRootHash,
                                                batch.txnRootHash)
