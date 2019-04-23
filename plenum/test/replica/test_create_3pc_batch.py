import functools

from common.serializers.serialization import invalid_index_serializer
from plenum.common.constants import POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID
from plenum.common.exceptions import InvalidClientMessageException

nodeCount = 4


def test_create_3pc_batch_with_empty_requests(replica):
    pp = replica.create_3pc_batch(0)
    assert pp is not None
    assert pp.reqIdr == tuple()


def test_create_3pc_batch(replica_with_requests, fake_requests, txn_roots, state_roots):
    replica_with_requests.consume_req_queue_for_pre_prepare = \
        lambda ledger, tm, view_no, pp_seq_no: (fake_requests, [], [])

    for (ledger_id, pp_seq_no, has_bls) in [
        (POOL_LEDGER_ID, 1, False),
        (DOMAIN_LEDGER_ID, 2, True),
        (CONFIG_LEDGER_ID, 3, True)
    ]:
        pre_prepare_msg = replica_with_requests.create_3pc_batch(ledger_id)

        assert pre_prepare_msg.poolStateRootHash == state_roots[POOL_LEDGER_ID]
        assert pre_prepare_msg.stateRootHash == state_roots[ledger_id]
        assert pre_prepare_msg.txnRootHash == txn_roots[ledger_id]
        assert pre_prepare_msg.ppSeqNo == pp_seq_no
        assert pre_prepare_msg.ledgerId == ledger_id
        assert pre_prepare_msg.viewNo == replica_with_requests.viewNo
        assert pre_prepare_msg.instId == replica_with_requests.instId
        assert pre_prepare_msg.reqIdr == tuple(req.digest for req in fake_requests)


def test_reqidr_ordered_regardless_validation_result(replica_with_requests, fake_requests):
    def randomDynamicValidation(self, req):
        req_keys = [fake_req.key for fake_req in fake_requests]
        if req_keys.index(req.key) % 2:
            raise InvalidClientMessageException('aaaaaaaa',
                                                req.reqId,
                                                "not valid req")

    replica_with_requests.node.doDynamicValidation = functools.partial(randomDynamicValidation,
                                                                       replica_with_requests.node)
    pre_prepare_msg = replica_with_requests.create_3pc_batch(DOMAIN_LEDGER_ID)
    invalid_from_pp = invalid_index_serializer.deserialize(pre_prepare_msg.discarded)
    assert len(invalid_from_pp) == len(fake_requests) / 2
    assert pre_prepare_msg.reqIdr == tuple(res.key for res in fake_requests)
