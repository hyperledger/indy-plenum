from plenum.common.eventually import eventually
from plenum.common.types import DOMAIN_LEDGER_ID
from plenum.test.helper import checkSufficientRepliesRecvd


def checkSufficientRepliesRecvdForReqs(looper, reqs, client, timeout):
    for req in reqs:
        looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                              req.reqId, 1, retryWait=1, timeout=timeout))


def checkNodesHaveSameRoots(nodes):
    stateRoots = set()
    txnRoots = set()
    for node in nodes:
        stateRoots.add(node.getState(DOMAIN_LEDGER_ID).headHash)
        txnRoots.add(node.domainLedger.uncommittedRootHash)

    assert len(stateRoots) == 1
    assert len(txnRoots) == 1

    stateRoots = set()
    txnRoots = set()
    for node in nodes:
        stateRoots.add(node.getState(DOMAIN_LEDGER_ID).committedHeadHash)
        txnRoots.add(node.domainLedger.root_hash)

    assert len(stateRoots) == 1
    assert len(txnRoots) == 1
