from plenum.common.txn_util import get_req_id, get_from, get_type
from stp_core.loop.eventually import eventually
from plenum.common.constants import TXN_TYPE
from plenum.common.types import f
from plenum.test import waits


def checkReplyIsPersisted(nodes, lpr, reply1):
    def chk(node):
        result = node.domainLedger.get(identifier=reply1.identifier,
                                       reqId=reply1.reqId)
        assert get_req_id(result) == reply1.reqId
        assert get_from(result) == reply1.identifier
        assert get_type(result) == reply1.operation.get(TXN_TYPE)

    timeout = waits.expectedPoolLedgerRepliedMsgPersisted(len(nodes))
    for node in nodes:
        lpr.run(eventually(chk, node, retryWait=1, timeout=timeout))
