from common.serializers.base58_serializer import Base58Serializer
from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.config_util import getConfig
from plenum.common.ledger import Ledger
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.request import Request
from plenum.common.util import get_utc_epoch
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.replica import Replica
from plenum.test.helper import init_discarded
from state.trie.pruning_trie import BLANK_ROOT
from stp_zmq.zstack import ZStack


def test_msg_len_limit_large_enough_for_preprepare():
    config = getConfig()

    batch_size = config.Max3PCBatchSize
    requests = [Request(signatures={})] * batch_size
    req_idr = [req.digest for req in requests]
    tm = get_utc_epoch()
    digest = OrderingService.generate_pp_digest(req_idr, 0, tm)
    state_root = Base58Serializer().serialize(BLANK_ROOT)
    txn_root = Ledger.hashToStr(CompactMerkleTree().root_hash)

    pp = PrePrepare(
        0,
        0,
        0,
        tm,
        req_idr,
        init_discarded(),
        digest,
        0,
        state_root,
        txn_root,
        0,
        True)

    assert len(ZStack.serializeMsg(pp)) <= config.MSG_LEN_LIMIT
