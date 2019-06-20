from enum import Enum
from typing import Dict, Optional

from plenum.common.channel import TxChannel, RxChannel, create_direct_channel, Router
from plenum.common.constants import POOL_LEDGER_ID, AUDIT_LEDGER_ID, AUDIT_TXN_LEDGERS_SIZE, AUDIT_TXN_LEDGER_ROOT, \
    CONFIG_LEDGER_ID
from plenum.common.ledger import Ledger
from plenum.common.metrics_collector import MetricsCollector
from plenum.common.timer import TimerService
from plenum.common.txn_util import get_payload_data
from plenum.server.catchup.ledger_leecher_service import LedgerLeecherService
from plenum.server.catchup.utils import CatchupDataProvider, LedgerCatchupComplete, NodeCatchupComplete, CatchupTill, \
    LedgerCatchupStart
from stp_core.common.log import getlogger

logger = getlogger()


class NodeLeecherService:
    class State(Enum):
        Idle = 0
        PreSyncingPool = 1
        SyncingAudit = 2
        SyncingPool = 3
        SyncingConfig = 4
        SyncingOthers = 5

    state_to_ledger = {
        State.PreSyncingPool: POOL_LEDGER_ID,
        State.SyncingAudit: AUDIT_LEDGER_ID,
        State.SyncingPool: POOL_LEDGER_ID,
        State.SyncingConfig: CONFIG_LEDGER_ID,
    }

    def __init__(self,
                 config: object,
                 input: RxChannel,
                 output: TxChannel,
                 timer: TimerService,
                 metrics: MetricsCollector,
                 provider: CatchupDataProvider):
        self._config = config
        self._input = input
        self._output = output
        self._timer = timer
        self.metrics = metrics
        self._provider = provider

        self._state = self.State.Idle
        self._catchup_till = {}  # type: Dict[int, CatchupTill]
        self._nodes_ledger_sizes = {}  # type: Dict[int, Dict[str, int]]

        # TODO: Get rid of this, theoretically most ledgers can be synced in parallel
        self._current_ledger = None  # type: Optional[int]

        self._leecher_outbox, self._leecher_outbox_rx = create_direct_channel()
        self._leecher_outbox_rx.subscribe(lambda msg: output.put_nowait(msg))
        router = Router(self._leecher_outbox_rx)
        router.add(LedgerCatchupStart, self._on_ledger_catchup_start)
        router.add(LedgerCatchupComplete, self._on_ledger_catchup_complete)

        self._leechers = {}  # type: Dict[int, LedgerLeecherService]

    def __repr__(self):
        return "{}:NodeLeecherService".format(self._provider.node_name())

    def register_ledger(self, ledger_id: int):
        self._leechers[ledger_id] = \
            LedgerLeecherService(ledger_id=ledger_id,
                                 config=self._config,
                                 input=self._input,
                                 output=self._leecher_outbox,
                                 timer=self._timer,
                                 metrics=self.metrics,
                                 provider=self._provider)

    def start(self, is_initial: bool = False):
        logger.info("{} starting catchup (is_initial={})".format(self, is_initial))

        for leecher in self._leechers.values():
            leecher.reset()

        self._catchup_till.clear()
        self._nodes_ledger_sizes.clear()
        if is_initial:
            self._enter_state(self.State.PreSyncingPool)
        else:
            self._enter_state(self.State.SyncingAudit)

    def num_txns_caught_up_in_last_catchup(self) -> int:
        return sum(leecher.num_txns_caught_up for leecher in self._leechers.values())

    def _on_ledger_catchup_start(self, msg: LedgerCatchupStart):
        self._nodes_ledger_sizes[msg.ledger_id] = msg.nodes_ledger_sizes

    def _on_ledger_catchup_complete(self, msg: LedgerCatchupComplete):
        if not self._validate_catchup_complete(msg):
            return

        if self._state == self.State.PreSyncingPool:
            self._enter_state(self.State.SyncingAudit)

        elif self._state == self.State.SyncingAudit:
            self._catchup_till = self._calc_catchup_till()
            self._enter_state(self.State.SyncingPool)

        elif self._state == self.State.SyncingPool:
            self._enter_state(self.State.SyncingConfig)

        elif self._state == self.State.SyncingConfig:
            self._enter_state(self.State.SyncingOthers)

        elif self._state == self.State.SyncingOthers:
            self._sync_next_ledger()

    def _validate_catchup_complete(self, msg: LedgerCatchupComplete):
        ledger_id = self.state_to_ledger.get(self._state)
        state_name = self._state.name
        if self._state == self.State.SyncingOthers:
            ledger_id = self._current_ledger
            state_name = "{}({})".format(state_name, ledger_id)

        if msg.ledger_id != ledger_id:
            logger.warning("{} got unexpected catchup complete {} during {}".
                           format(self, msg, state_name))
            return False

        return True

    def _sync_next_ledger(self):
        self._current_ledger = self._get_next_ledger(self._current_ledger)
        if self._current_ledger is not None:
            self._catchup_ledger(self._current_ledger)
        else:
            self._enter_state(self.State.Idle)
            self._output.put_nowait(NodeCatchupComplete())

    def _get_next_ledger(self, ledger_id: Optional[int]) -> Optional[int]:
        ledger_ids = list(self._leechers.keys())
        ledger_ids.remove(AUDIT_LEDGER_ID)
        ledger_ids.remove(POOL_LEDGER_ID)
        ledger_ids.remove(CONFIG_LEDGER_ID)

        if len(ledger_ids) == 0:
            return None

        if ledger_id is None:
            return ledger_ids[0]

        next_index = ledger_ids.index(ledger_id) + 1
        if next_index == len(ledger_ids):
            return None

        return ledger_ids[next_index]

    def _enter_state(self, state: State):
        logger.info("{} transitioning from {} to {}".format(self, self._state.name, state.name))

        self._state = state
        if state == self.State.Idle:
            return

        if state == self.State.PreSyncingPool:
            self._leechers[POOL_LEDGER_ID].start(request_ledger_statuses=False)
            return

        if state == self.State.SyncingOthers:
            self._sync_next_ledger()
            return

        self._catchup_ledger(self.state_to_ledger[state])

    def _catchup_ledger(self, ledger_id: int):
        leecher = self._leechers[ledger_id]
        catchup_till = self._catchup_till.get(ledger_id)
        if catchup_till is None:
            leecher.start()
        else:
            leecher.start(till=catchup_till,
                          nodes_ledger_sizes=self._calc_nodes_ledger_sizes(ledger_id))

    def _calc_catchup_till(self) -> Dict[int, CatchupTill]:
        audit_ledger = self._provider.ledger(AUDIT_LEDGER_ID)
        last_audit_txn = audit_ledger.get_last_committed_txn()
        if last_audit_txn is None:
            return {}

        catchup_till = {}
        last_audit_txn = get_payload_data(last_audit_txn)
        for ledger_id, final_size in last_audit_txn[AUDIT_TXN_LEDGERS_SIZE].items():
            ledger = self._provider.ledger(ledger_id)
            if ledger is None:
                logger.warning("{} has audit ledger with references to nonexistent ledger with ID {}".
                               format(self, ledger_id))
                continue
            start_size = ledger.size

            final_hash = last_audit_txn[AUDIT_TXN_LEDGER_ROOT].get(ledger_id)
            if final_hash is None:
                if final_size != ledger.size:
                    logger.error("{} has corrupted audit ledger: "
                                 "it indicates that ledger {} has new transactions but doesn't have new txn root".
                                 format(self, ledger_id))
                    return {}
                final_hash = Ledger.hashToStr(ledger.tree.root_hash) if final_size > 0 else None

            if isinstance(final_hash, int):
                audit_txn = audit_ledger.getBySeqNo(audit_ledger.size - final_hash)
                if audit_txn is None:
                    logger.error("{} has corrupted audit ledger: "
                                 "its txn root for ledger {} references nonexistent txn with seq_no {} - {} = {}".
                                 format(self, ledger_id, audit_ledger.size, final_hash, audit_ledger.size - final_hash))
                    return {}

                audit_txn = get_payload_data(audit_txn)
                final_hash = audit_txn[AUDIT_TXN_LEDGER_ROOT].get(ledger_id)
                if not isinstance(final_hash, str):
                    logger.error("{} has corrupted audit ledger: "
                                 "its txn root for ledger {} references txn with seq_no {} - {} = {} "
                                 "which doesn't contain txn root".
                                 format(self, ledger_id, audit_ledger.size, final_hash, audit_ledger.size - final_hash))
                    return {}

            catchup_till[ledger_id] = CatchupTill(start_size=start_size,
                                                  final_size=final_size,
                                                  final_hash=final_hash)

        return catchup_till

    def _calc_nodes_ledger_sizes(self, ledger_id: int) -> Dict[str, int]:
        result = self._nodes_ledger_sizes.get(ledger_id)
        if result is not None:
            return result

        nodes_audit_size = self._nodes_ledger_sizes[AUDIT_LEDGER_ID]
        if nodes_audit_size is None:
            return {}

        result = {}
        audit_ledger = self._provider.ledger(AUDIT_LEDGER_ID)
        for node_id, audit_seq_no in nodes_audit_size.items():
            # It can happen so that during catching up audit ledger we caught up
            # less transactions than some nodes reported
            audit_seq_no = min(audit_seq_no, audit_ledger.size)

            audit_txn = audit_ledger.getBySeqNo(audit_seq_no)
            audit_txn = get_payload_data(audit_txn)
            # Not having a reference to some ledger in audit txn can be a valid
            # case if we just installed a plugin that adds a new ledger, but
            # no audit txns were written yet
            ledger_size = audit_txn[AUDIT_TXN_LEDGERS_SIZE].get(ledger_id, 0)

            result[node_id] = ledger_size

        return result
