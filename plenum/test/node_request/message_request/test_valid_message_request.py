import pytest

from plenum.common.constants import LEDGER_STATUS, CONSISTENCY_PROOF, \
    PREPREPARE
from plenum.common.messages.node_messages import MessageReq, ChooseField, \
    AnyMapField, MessageRep, AnyField
from plenum.common.types import f
from plenum.test.helper import countDiscarded
from stp_core.loop.eventually import eventually


invalid_type_discard_log = "unknown value 'invalid_type'"
invalid_value_discard_log = "cannot serve request"


whitelist = [invalid_type_discard_log, ]


patched_schema = (
    (f.MSG_TYPE.nm, ChooseField(values={'invalid_type', LEDGER_STATUS,
                                        CONSISTENCY_PROOF,
                                        PREPREPARE})),
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


def fill_counters(nodes, log_message):
    global discard_counts
    discard_counts[log_message] = {n.name: countDiscarded(n, log_message)
                      for n in nodes}


def chk(nodes, log_message):
    global discard_counts
    for n in nodes:
        assert countDiscarded(n, log_message) > discard_counts[log_message][n.name]


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

    looper.run(eventually(chk, other_nodes, invalid_type_discard_log, retryWait=1))

    fill_counters(other_nodes, invalid_type_discard_log)

    bad_msg = patched_MessageRep()('invalid_type', {'p1': 'v1', 'p2': 'v2'},
                                   {'some_message': 'message'})
    bad_node.send(bad_msg)
    looper.run(eventually(chk, other_nodes, invalid_type_discard_log, retryWait=1))


def test_node_reject_invalid_req_resp_params(looper, nodes):
    """
    Node does not accept invalid `MessageReq`, with missing params.
    Also it does not accept invalid `MessageRep`
    """
    global discard_counts
    bad_node, other_nodes = nodes

    bad_msgs = [
        patched_MessageReq()(LEDGER_STATUS, {'p1': 'v1', 'p2': 'v2'}),
        patched_MessageReq()(CONSISTENCY_PROOF, {f.LEDGER_ID.nm: 1,
                                                  f.SEQ_NO_START.nm: 10}),
        patched_MessageReq()(PREPREPARE, {f.INST_ID.nm: 1,
                                          f.VIEW_NO.nm: 0,
                                          f.SEQ_NO_START.nm: 10}),
        patched_MessageReq()(PREPREPARE, {f.INST_ID.nm: -1,
                                          f.VIEW_NO.nm: 1,
                                          f.PP_SEQ_NO.nm: 10}),
        patched_MessageReq()(LEDGER_STATUS, {f.LEDGER_ID.nm: 100}),
    ]

    for bad_msg in bad_msgs:
        fill_counters(other_nodes, invalid_value_discard_log)
        bad_node.send(bad_msg)
        looper.run(eventually(chk, other_nodes, invalid_value_discard_log,
                              retryWait=1))
