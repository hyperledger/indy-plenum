from binascii import hexlify

from stp_core.loop.eventually import eventually
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.helper import waitForSufficientRepliesForRequests


def checkSufficientRepliesRecvdForReqs(looper, reqs, client, timeout):
    for req in reqs:
        looper.run(eventually(waitForSufficientRepliesForRequests,
                              looper,
                              client,
                              requests=[req, ],
                              fVal=1,
                              customTimeoutPerReq=timeout))


def checkNodesHaveSameRoots(nodes, checkUnCommitted=True,
                            checkCommitted=True):
    def addRoot(root, collection):
        if root:
            collection.add(hexlify(root))
        else:
            collection.add(root)

    if checkUnCommitted:
        stateRoots = set()
        txnRoots = set()
        for node in nodes:
            addRoot(node.getState(DOMAIN_LEDGER_ID).headHash, stateRoots)
            addRoot(node.getLedger(DOMAIN_LEDGER_ID).uncommittedRootHash,
                    txnRoots)

        assert len(stateRoots) == 1
        assert len(txnRoots) == 1

    if checkCommitted:
        stateRoots = set()
        txnRoots = set()
        for node in nodes:
            addRoot(node.getState(DOMAIN_LEDGER_ID).committedHeadHash,
                    stateRoots)
            addRoot(node.getLedger(DOMAIN_LEDGER_ID).tree.root_hash,
                    txnRoots)

        assert len(stateRoots) == 1
        assert len(txnRoots) == 1
