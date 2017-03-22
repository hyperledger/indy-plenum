from plenum.common.eventually import eventually
from plenum.common.constants import TXN_TYPE
from plenum.common.types import f


def checkReplyIsPersisted(nodes, lpr, reply1):
    def chk(node):
        result = node.domainLedger.get(identifier=reply1.identifier,
                                       reqId=reply1.reqId)
        assert result.get(f.REQ_ID.nm) == reply1.reqId
        assert result.get(f.IDENTIFIER.nm) == reply1.identifier
        assert result.get(TXN_TYPE) == reply1.operation.get(TXN_TYPE)

    for node in nodes:
        lpr.run(eventually(chk, node, retryWait=1, timeout=5))
