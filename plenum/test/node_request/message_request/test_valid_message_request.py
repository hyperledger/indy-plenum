import pytest

from plenum.common.constants import LEDGER_STATUS, CONSISTENCY_PROOF, \
    PREPREPARE, PROPAGATE
from plenum.common.messages.node_messages import MessageReq, ChooseField, \
    AnyMapField, MessageRep, AnyField, LedgerStatus, ConsistencyProof, \
    PrePrepare, Propagate
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.test.helper import countDiscarded
from stp_core.loop.eventually import eventually


invalid_type_discard_log = "unknown value 'invalid_type'"
invalid_req_discard_log = "cannot serve request"
invalid_rep_discard_log = "cannot process requested message response"


whitelist = [invalid_type_discard_log, ]


patched_schema = (
    (f.MSG_TYPE.nm, ChooseField(values={'invalid_type', LEDGER_STATUS,
                                        CONSISTENCY_PROOF, PREPREPARE,
                                        PROPAGATE})),
    (f.PARAMS.nm, AnyMapField())
)


def patched_MessageReq():
    class PMessageReq(MessageReq):
        schema = patched_schema
    return PMessageReq


def patched_MessageRep():
    class PMessageRep(MessageRep):
        schema = (
            *patched_schema,
            (f.MSG.nm, AnyField())
        )
    return PMessageRep


discard_counts = {}

pre_prepare_msg = PrePrepare(
    0,
    1,
    3,
    get_utc_epoch(),
    [['4AdS22kC7xzb4bcqg9JATuCfAMNcQYcZa1u5eWzs6cSJ', 1499707723017300]],
    1,
    'f99937241d4c891c08e92a3cc25966607315ca66b51827b170d492962d58a9be',
    1,
    'CZecK1m7VYjSNCC7pGHj938DSW2tfbqoJp1bMJEtFqvG',
    '7WrAMboPTcMaQCU1raoj28vnhu2bPMMd2Lr9tEcsXeCJ',
)

propagate_msg = Propagate(**{'request': {'identifier': '5rArie7XKukPCaEwq5XGQJnM9Fc5aZE3M9HAPVfMU2xC',
                                         'signature': 'ZbZG68WiaK67eU3CsgpVi85jpgCztW9Yqe7D5ezDUfWbKdiPPVbWq4Tb5m4Ur3jcR5wJ8zmBUZXZudjvMN63Aa9',
                                         'operation': {'amount': 62,
                                                       'type': 'buy'},
                                         'reqId': 1499782864169193},
                             'senderClient': '+DG1:vO9#de6?R?>:3RwdAXSdefgLLfxSoN4WMEe'})

bad_msgs = [
    (LEDGER_STATUS, {'p1': 'v1', 'p2': 'v2'}, LedgerStatus(
        1, 20, 1, 2, '77wuDUSr4FtAJzJbSqSW7bBw8bKAbra8ABSAjR72Nipq')),
    (LEDGER_STATUS, {f.LEDGER_ID.nm: 100}, LedgerStatus(
        1, 20, 1, 2, '77wuDUSr4FtAJzJbSqSW7bBw8bKAbra8ABSAjR72Nipq')),
    (CONSISTENCY_PROOF, {f.LEDGER_ID.nm: 1, f.SEQ_NO_START.nm: 10},
     ConsistencyProof(1, 2, 20, 1, 3,
                      'BvmagFYpXAYNTuNW8Qssk9tMhEEPucLqL55YuwngUvMw',
                      'Dce684wcwhV2wNZCuYTzdW9Kr13ZXFgiuAuAGibFZc4v',
                      ['58qasGZ9y3TB1pMz7ARKjJeccEbvbx6FT6g3NFnjYsTS'])),
    (PREPREPARE, {f.INST_ID.nm: 1, f.VIEW_NO.nm: 0, f.SEQ_NO_START.nm: 10},
     pre_prepare_msg),
    (PREPREPARE, {f.INST_ID.nm: -1, f.VIEW_NO.nm: 1, f.PP_SEQ_NO.nm: 10},
     pre_prepare_msg),
    (PROPAGATE, {f.IDENTIFIER.nm: 'aa', f.REQ_ID.nm: 'fr'}, propagate_msg),
    (PROPAGATE, {
        f.IDENTIFIER.nm: '4AdS22kC7xzb4bcqg9JATuCfAMNcQYcZa1u5eWzs6cSJ'}, propagate_msg),
    (PROPAGATE, {f.REQ_ID.nm: 1499707723017300}, propagate_msg),
]


def fill_counters(nodes, log_message):
    global discard_counts
    discard_counts[log_message] = {n.name: countDiscarded(n, log_message)
                                   for n in nodes}


def chk(nodes, log_message):
    global discard_counts
    for n in nodes:
        assert countDiscarded(
            n, log_message) > discard_counts[log_message][n.name]


@pytest.fixture(scope='module')
def nodes(txnPoolNodeSet):
    bad_node = txnPoolNodeSet[-1]
    other_nodes = [n for n in txnPoolNodeSet if n != bad_node]
    return bad_node, other_nodes


def test_node_reject_invalid_req_resp_type(looper, nodes):
    """
    Node does not accept invalid `MessageReq`, with an unacceptable type. Also
    it does not accept invalid `MessageRep`
    """
    global discard_counts
    bad_node, other_nodes = nodes
    fill_counters(other_nodes, invalid_type_discard_log)
    bad_msg = patched_MessageReq()('invalid_type', {'p1': 'v1', 'p2': 'v2'})
    bad_node.send(bad_msg)

    looper.run(eventually(chk, other_nodes,
                          invalid_type_discard_log, retryWait=1))

    fill_counters(other_nodes, invalid_type_discard_log)

    bad_msg = patched_MessageRep()('invalid_type', {'p1': 'v1', 'p2': 'v2'},
                                   {'some_message': 'message'})
    bad_node.send(bad_msg)
    looper.run(eventually(chk, other_nodes,
                          invalid_type_discard_log, retryWait=1))


def test_node_reject_invalid_req_params(looper, nodes):
    """
    Node does not accept invalid `MessageReq`, with missing params.
    Also it does not accept invalid `MessageRep`
    """
    global discard_counts, bad_msgs
    bad_node, other_nodes = nodes

    for bad_msg in bad_msgs:
        fill_counters(other_nodes, invalid_req_discard_log)
        bad_node.send(patched_MessageReq()(*bad_msg[:2]))
        looper.run(eventually(chk, other_nodes, invalid_req_discard_log,
                              retryWait=1))


def test_node_reject_invalid_resp_params(looper, nodes):
    """
    Node does not accept invalid `MessageReq`, with missing params.
    Also it does not accept invalid `MessageRep`
    """
    global discard_counts, bad_msgs
    bad_node, other_nodes = nodes

    for bad_msg in bad_msgs:
        fill_counters(other_nodes, invalid_rep_discard_log)
        bad_node.send(patched_MessageRep()(*bad_msg))
        looper.run(eventually(chk, other_nodes, invalid_rep_discard_log,
                              retryWait=1))
