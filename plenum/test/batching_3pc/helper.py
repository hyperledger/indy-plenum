import types
from binascii import hexlify

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import ThreePhaseType
from plenum.common.startable import Mode
from plenum.common.txn_util import reqToTxn, append_txn_metadata
from plenum.common.util import check_if_all_equal_in_list
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.catchup.catchup_rep_service import LedgerCatchupComplete
from plenum.server.catchup.utils import NodeCatchupComplete


def checkNodesHaveSameRoots(nodes, checkUnCommitted=True,
                            checkCommitted=True,
                            checkLastOrderedPpSeqNo=True,
                            checkSeqNoDb=True):
    def addRoot(root, collection):
        if root:
            collection.add(hexlify(root))
        else:
            collection.add(root)

    if checkLastOrderedPpSeqNo:
        ppSeqNos = set()
        for node in nodes:
            ppSeqNos.add(node.replicas[0].last_ordered_3pc)

        assert len(ppSeqNos) == 1

    if checkSeqNoDb:
        seqNoSizes = set()
        for node in nodes:
            seqNoSizes.add(node.seqNoDB.size)

        assert len(seqNoSizes) == 1

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


def add_txns_to_ledger_before_order(replica, reqs):
    replica.added = False
    origMethod = replica.tryOrder

    def tryOrderAndAddTxns(self, commit):
        canOrder, _ = self.canOrder(commit)
        node = replica.node
        if not replica.added and canOrder:
            pp = self.getPrePrepare(commit.viewNo, commit.ppSeqNo)
            ledger_manager = node.ledgerManager
            ledger_id = DOMAIN_LEDGER_ID
            catchup_rep_service = ledger_manager._node_leecher._leechers[ledger_id]._catchup_rep_service

            # simulate audit ledger catchup
            three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare=pp,
                                                           valid_txn_count=len(reqs),
                                                           state_root=pp.stateRootHash,
                                                           txn_root=pp.txnRootHash)
            node.audit_handler.post_batch_applied(three_pc_batch)
            node.audit_handler.commit_batch(ledger_id, len(reqs), pp.stateRootHash, pp.txnRootHash, pp.ppTime)

            ledger_manager.preCatchupClbk(ledger_id)
            pp = self.getPrePrepare(commit.viewNo, commit.ppSeqNo)
            for req in reqs:
                txn = append_txn_metadata(reqToTxn(req), txn_time=pp.ppTime)
                catchup_rep_service._add_txn(txn)
            ledger_manager._on_ledger_sync_complete(LedgerCatchupComplete(
                ledger_id=DOMAIN_LEDGER_ID,
                num_caught_up=len(reqs),
                last_3pc=(node.viewNo, commit.ppSeqNo)))
            ledger_manager._on_catchup_complete(NodeCatchupComplete())
            replica.added = True

        return origMethod(commit)

    replica.tryOrder = types.MethodType(tryOrderAndAddTxns, replica)


def start_precatchup_before_order(replica):
    called = False
    origMethod = replica.tryOrder

    def tryOrderAndAddTxns(self, commit):
        nonlocal called
        canOrder, _ = self.canOrder(commit)

        if not called and canOrder:
            ledger_manager = replica.node.ledgerManager
            ledger_manager.preCatchupClbk(DOMAIN_LEDGER_ID)
            called = True

        return origMethod(commit)

    replica.tryOrder = types.MethodType(tryOrderAndAddTxns, replica)


def make_node_syncing(replica, three_phase_type: ThreePhaseType):
    added = False

    def specificPrePrepares(wrappedMsg):
        msg, sender = wrappedMsg
        nonlocal added
        node = replica.node
        if isinstance(msg, three_phase_type) and not added:
            node.mode = Mode.syncing
            added = True
        return 0

    replica.node.nodeIbStasher.delay(specificPrePrepares)


def fail_on_execute_batch_on_master(node):
    def fail_process_ordered(self, ordered):
        if ordered.instId == 0:
            raise Exception('Should not process Ordered at this point')

    node.processOrdered = types.MethodType(fail_process_ordered, node)


def check_uncommitteds_equal(nodes):
    t_roots = [node.domainLedger.uncommittedRootHash for node in nodes]
    s_roots = [node.states[DOMAIN_LEDGER_ID].headHash for node in nodes]
    assert check_if_all_equal_in_list(t_roots)
    assert check_if_all_equal_in_list(s_roots)
    return t_roots[0], s_roots[0]


def node_caughtup(node, old_count):
    assert node.spylog.count(node.allLedgersCaughtUp) > old_count
