import copy
from collections import deque
from logging import getLogger
from typing import NamedTuple

from plenum.common.constants import POOL_LEDGER_ID, ALIAS, SERVICES, VALIDATOR, NODE, DATA, AUDIT_LEDGER_ID, \
    AUDIT_TXN_NODE_REG, TYPE, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PRIMARIES, AUDIT_TXN_LEDGERS_SIZE
from plenum.common.event_bus import InternalBus
from plenum.common.messages.internal_messages import VoteForViewChange
from plenum.common.request import Request
from plenum.common.txn_util import get_type, get_payload_data, get_seq_no
from plenum.common.util import SortedDict
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.suspicion_codes import Suspicions

UncommittedNodeReg = NamedTuple('UncommittedNodeReg',
                                [('uncommitted_node_reg', list),
                                 ('view_no', int)])
logger = getLogger()


class NodeRegHandler(BatchRequestHandler, WriteRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        BatchRequestHandler.__init__(self, database_manager, POOL_LEDGER_ID)
        WriteRequestHandler.__init__(self, database_manager, NODE, POOL_LEDGER_ID)

        self.uncommitted_node_reg = []
        self.committed_node_reg = []

        # committed node reg at the beginning of view
        # matches the committed node reg BEFORE the first txn in a view is applied (that is according to the last txn in the last view)
        self.committed_node_reg_at_beginning_of_view = SortedDict()

        # uncommitted node reg at the beginning of view
        # matches the uncommittednode reg BEFORE the first txn in a view is applied (that is according to the last txn in the last view)
        self.uncommitted_node_reg_at_beginning_of_view = SortedDict()

        self._uncommitted = deque()  # type: deque[UncommittedNodeReg]
        self._uncommitted_view_no = 0
        self._committed_view_no = 0

        self.internal_bus = None  # type: InternalBus

    def set_internal_bus(self, internal_bus: InternalBus):
        self.internal_bus = internal_bus

    @property
    def active_node_reg(self):
        if not self.uncommitted_node_reg_at_beginning_of_view:
            return []
        return self.uncommitted_node_reg_at_beginning_of_view.peekitem(-1)[1]

    def on_catchup_finished(self):
        self._load_current_node_reg()
        # we must have node regs for at least last two views
        self._load_last_view_node_reg()
        self.uncommitted_node_reg_at_beginning_of_view = copy.deepcopy(self.committed_node_reg_at_beginning_of_view)
        logger.info("Loaded current node registry from the ledger: {}".format(self.uncommitted_node_reg))
        logger.info(
            "Current committed node registry for previous views: {}".format(
                sorted(self.committed_node_reg_at_beginning_of_view.items())))
        logger.info(
            "Current uncommitted node registry for previous views: {}".format(
                sorted(self.uncommitted_node_reg_at_beginning_of_view.items())))
        logger.info("Current active node registry: {}".format(self.active_node_reg))

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        # Observer case:
        if not self.uncommitted_node_reg and three_pc_batch.node_reg:
            self.uncommitted_node_reg = list(three_pc_batch.node_reg)

        view_no = three_pc_batch.view_no if three_pc_batch.original_view_no is None else three_pc_batch.original_view_no

        # Update active_node_reg to point to node_reg at the end of last view
        if view_no > self._uncommitted_view_no:
            self.uncommitted_node_reg_at_beginning_of_view[view_no] = list(
                self._uncommitted[-1].uncommitted_node_reg) if len(self._uncommitted) > 0 else list(
                self.committed_node_reg)
            self._uncommitted_view_no = view_no

        self._uncommitted.append(UncommittedNodeReg(list(self.uncommitted_node_reg), view_no))

        three_pc_batch.node_reg = list(self.uncommitted_node_reg)

        logger.debug("Applied uncommitted node registry: {}".format(self.uncommitted_node_reg))
        logger.debug(
            "Current committed node registry for previous views: {}".format(
                sorted(self.committed_node_reg_at_beginning_of_view.items())))
        logger.debug(
            "Current uncommitted node registry for previous views: {}".format(
                sorted(self.uncommitted_node_reg_at_beginning_of_view.items())))
        logger.debug("Current active node registry: {}".format(self.active_node_reg))

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        reverted = self._uncommitted.pop()
        if len(self._uncommitted) == 0:
            self.uncommitted_node_reg = list(self.committed_node_reg)
            self._uncommitted_view_no = self._committed_view_no
        else:
            last_uncommitted = self._uncommitted[-1]
            self.uncommitted_node_reg = last_uncommitted.uncommitted_node_reg
            self._uncommitted_view_no = last_uncommitted.view_no

        # find the uncommitted node reg at the beginning of view
        if self._uncommitted_view_no < reverted.view_no:
            self.uncommitted_node_reg_at_beginning_of_view.pop(reverted.view_no)

        logger.debug("Reverted uncommitted node registry from {} to {}".format(reverted.uncommitted_node_reg,
                                                                               self.uncommitted_node_reg))
        logger.debug(
            "Current committed node registry for previous views: {}".format(
                sorted(self.committed_node_reg_at_beginning_of_view.items())))
        logger.debug(
            "Current uncommitted node registry for previous views: {}".format(
                sorted(self.uncommitted_node_reg_at_beginning_of_view.items())))
        logger.debug("Current active node registry: {}".format(self.active_node_reg))

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        # 1. Update node_reg_at_beginning_of_view first (to match the node reg at the end of last view)
        three_pc_batch_view_no = three_pc_batch.view_no if three_pc_batch.original_view_no is None else three_pc_batch.original_view_no
        if three_pc_batch_view_no > self._committed_view_no:
            self.committed_node_reg_at_beginning_of_view[three_pc_batch_view_no] = list(self.committed_node_reg)
            self._committed_view_no = three_pc_batch_view_no

            self._gc_node_reg_at_beginning_of_view(self.committed_node_reg_at_beginning_of_view)
            self._gc_node_reg_at_beginning_of_view(self.uncommitted_node_reg_at_beginning_of_view)

        # 2. update committed node reg
        prev_committed = self.committed_node_reg
        self.committed_node_reg = self._uncommitted.popleft().uncommitted_node_reg

        # trigger view change if nodes count changed
        # TODO: create a new message to pass Suspicious events and make ViewChangeTriggerService the only place for
        # view change triggering
        if self.internal_bus and len(prev_committed) != len(self.committed_node_reg):
            self.internal_bus.send(VoteForViewChange(Suspicions.NODE_COUNT_CHANGED, three_pc_batch_view_no + 1))

        if prev_committed != self.committed_node_reg:
            logger.info("Committed node registry: {}".format(self.committed_node_reg))
            logger.info(
                "Current committed node registry for previous views: {}".format(
                    sorted(self.committed_node_reg_at_beginning_of_view.items())))
            logger.info(
                "Current uncommitted node registry for previous views: {}".format(
                    sorted(self.uncommitted_node_reg_at_beginning_of_view.items())))
            logger.info("Current active node registry: {}".format(self.active_node_reg))
        else:
            logger.debug("Committed node registry: {}".format(self.committed_node_reg))
            logger.debug(
                "Current committed node registry for previous views: {}".format(
                    sorted(self.committed_node_reg_at_beginning_of_view.items())))
            logger.debug(
                "Current uncommitted node registry for previous views: {}".format(
                    sorted(self.uncommitted_node_reg_at_beginning_of_view.items())))
            logger.debug("Current active node registry: {}".format(self.active_node_reg))

    def _gc_node_reg_at_beginning_of_view(self, node_reg):
        # make sure that we have node reg for the current and previous view (which can be less than the current for more than 1)
        # Ex.: node_reg_at_beginning_of_view has views {0, 3, 5, 7, 11, 13), committed is now 7, so we need to keep all uncommitted (11, 13),
        # and keep the one from the previous view (5). Views 0 and 3 needs to be deleted.
        committed_view_nos = list(node_reg.keys())
        prev_committed_index = max(committed_view_nos.index(self._committed_view_no) - 1, 0) \
            if self._committed_view_no in node_reg else 0
        for view_no in committed_view_nos[:prev_committed_index]:
            node_reg.pop(view_no, None)

    def apply_request(self, request: Request, batch_ts, prev_result):
        if request.operation.get(TYPE) != NODE:
            return None, None, None

        node_name = request.operation[DATA][ALIAS]
        services = request.operation[DATA].get(SERVICES)

        if services is None:
            return None, None, None

        if node_name not in self.uncommitted_node_reg and VALIDATOR in services:
            # new node added or old one promoted
            self.uncommitted_node_reg.append(node_name)
            logger.info("Changed uncommitted node registry to: {}".format(self.uncommitted_node_reg))
        elif node_name in self.uncommitted_node_reg and VALIDATOR not in services:
            # existing node demoted
            self.uncommitted_node_reg.remove(node_name)
            logger.info("Changed uncommitted node registry to: {}".format(self.uncommitted_node_reg))

        return None, None, None

    def update_state(self, txn, prev_result, request, is_committed=False):
        pass

    def static_validation(self, request):
        pass

    def dynamic_validation(self, request, req_pp_time):
        pass

    def gen_state_key(self, txn):
        pass

    def _load_current_node_reg(self):
        node_reg = self.__load_current_node_reg_from_audit_ledger()
        if node_reg is None:
            node_reg = self.__load_node_reg_from_pool_ledger()
        self.uncommitted_node_reg = list(node_reg)
        self.committed_node_reg = list(node_reg)

    def _load_last_view_node_reg(self):
        self.committed_node_reg_at_beginning_of_view.clear()

        # 1. check if we have audit ledger at all
        audit_ledger = self.database_manager.get_ledger(AUDIT_LEDGER_ID)
        if not audit_ledger:
            # don't have audit ledger yet, so get aleady loaded values from the pool ledger
            self.committed_node_reg_at_beginning_of_view[0] = list(self.uncommitted_node_reg)
            self._committed_view_no = 0
            self._uncommitted_view_no = 0
            return

        # 2. get the first txn in the current view and last txn in the last view
        first_txn_in_this_view, last_txn_in_prev_view = self.__get_first_txn_in_view_from_audit(audit_ledger,
                                                                                                audit_ledger.get_last_committed_txn())

        # 3. set view_no
        self._committed_view_no = get_payload_data(first_txn_in_this_view)[AUDIT_TXN_VIEW_NO]
        self._uncommitted_view_no = self._committed_view_no

        # 4. Use last txn in last view to get the node reg
        # get from pool ledger if there is no txns for last view in audit
        if last_txn_in_prev_view is None:
            node_reg_this_view = self.__load_node_reg_for_first_audit_txn(first_txn_in_this_view)
        else:
            node_reg_this_view = list(self.__load_node_reg_from_audit_txn(audit_ledger, last_txn_in_prev_view))
        self.committed_node_reg_at_beginning_of_view[self._committed_view_no] = node_reg_this_view

        # 5. Check if audit ledger has information about 0 view only
        if self._committed_view_no == 0:
            return

        # 5. If audit has just 1 txn for the current view (and this view >0), then
        # get the last view from the pool ledger
        if last_txn_in_prev_view is None:
            # assume last view=0 if we don't know it
            self.committed_node_reg_at_beginning_of_view[0] = list(
                self.__load_node_reg_for_first_audit_txn(first_txn_in_this_view))
            return

        # 6. Get the first audit txn for the last view
        first_txn_in_last_view, last_txn_in_pre_last_view = self.__get_first_txn_in_view_from_audit(audit_ledger,
                                                                                                    last_txn_in_prev_view)

        # 7. Use last txn in the view before the last one to get the node reg
        # get from pool ledger if there is no txns for view before the last one in audit
        if last_txn_in_pre_last_view is None:
            node_reg_last_view = self.__load_node_reg_for_first_audit_txn(first_txn_in_last_view)
        else:
            node_reg_last_view = list(self.__load_node_reg_from_audit_txn(audit_ledger, last_txn_in_pre_last_view))
        last_view_no = get_payload_data(first_txn_in_last_view)[AUDIT_TXN_VIEW_NO]
        self.committed_node_reg_at_beginning_of_view[last_view_no] = node_reg_last_view

    def __load_node_reg_from_pool_ledger(self, to=None):
        node_reg = []
        for _, txn in self.ledger.getAllTxn(to=to):
            if get_type(txn) != NODE:
                continue
            txn_data = get_payload_data(txn)
            node_name = txn_data[DATA][ALIAS]
            services = txn_data[DATA].get(SERVICES)

            if services is None:
                continue

            if node_name not in node_reg and VALIDATOR in services:
                # new node added or old one promoted
                node_reg.append(node_name)
            elif node_name in node_reg and VALIDATOR not in services:
                # existing node demoted
                node_reg.remove(node_name)
        return node_reg

    # TODO: create a helper class to get data from Audit Ledger
    def __load_current_node_reg_from_audit_ledger(self):
        audit_ledger = self.database_manager.get_ledger(AUDIT_LEDGER_ID)
        if not audit_ledger:
            return None

        last_txn = audit_ledger.get_last_committed_txn()
        last_txn_node_reg = get_payload_data(last_txn).get(AUDIT_TXN_NODE_REG)
        if last_txn_node_reg is None:
            return None

        if isinstance(last_txn_node_reg, int):
            seq_no = get_seq_no(last_txn) - last_txn_node_reg
            audit_txn_for_seq_no = audit_ledger.getBySeqNo(seq_no)
            last_txn_node_reg = get_payload_data(audit_txn_for_seq_no).get(AUDIT_TXN_NODE_REG)

        if last_txn_node_reg is None:
            return None
        return last_txn_node_reg

    def __load_node_reg_from_audit_txn(self, audit_ledger, audit_txn):
        audit_txn_data = get_payload_data(audit_txn)

        # Get the node reg from audit txn
        node_reg = audit_txn_data.get(AUDIT_TXN_NODE_REG)
        if node_reg is None:
            return self.__load_node_reg_for_first_audit_txn(audit_txn)

        if isinstance(node_reg, int):
            seq_no = get_seq_no(audit_txn) - node_reg
            prev_audit_txn = audit_ledger.getBySeqNo(seq_no)
            node_reg = get_payload_data(prev_audit_txn).get(AUDIT_TXN_NODE_REG)

        if node_reg is None:
            return self.__load_node_reg_for_first_audit_txn(audit_txn)

        return node_reg

    def __get_first_txn_in_view_from_audit(self, audit_ledger, this_view_first_txn):
        '''
        :param audit_ledger: audit ledger
        :param this_view_first_txn: a txn from the current view
        :return: the first txn in this view and the last txn in the previous view (if amy, otherwise None)
        '''
        this_txn_view_no = get_payload_data(this_view_first_txn).get(AUDIT_TXN_VIEW_NO)

        prev_view_last_txn = None
        while True:
            txn_primaries = get_payload_data(this_view_first_txn).get(AUDIT_TXN_PRIMARIES)
            if isinstance(txn_primaries, int):
                seq_no = get_seq_no(this_view_first_txn) - txn_primaries
                this_view_first_txn = audit_ledger.getBySeqNo(seq_no)
            this_txn_seqno = get_seq_no(this_view_first_txn)
            if this_txn_seqno <= 1:
                break
            prev_view_last_txn = audit_ledger.getBySeqNo(this_txn_seqno - 1)
            prev_txn_view_no = get_payload_data(prev_view_last_txn).get(AUDIT_TXN_VIEW_NO)

            if this_txn_view_no != prev_txn_view_no:
                break

            this_view_first_txn = prev_view_last_txn
            prev_view_last_txn = None

        return this_view_first_txn, prev_view_last_txn

    def __load_node_reg_for_first_audit_txn(self, first_audit_txn):
        # If this is the first txn in the audit ledger, so that we don't know a full history,
        # then get node reg from the pool ledger
        audit_txn_data = get_payload_data(first_audit_txn)
        genesis_pool_ledger_size = audit_txn_data[AUDIT_TXN_LEDGERS_SIZE][POOL_LEDGER_ID]
        return self.__load_node_reg_from_pool_ledger(to=genesis_pool_ledger_size)
