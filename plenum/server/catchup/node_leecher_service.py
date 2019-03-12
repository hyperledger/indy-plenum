from enum import Enum
from typing import Dict, Optional

from plenum.common.channel import TxChannel, RxChannel, create_direct_channel, Router
from plenum.common.constants import POOL_LEDGER_ID, AUDIT_LEDGER_ID
from plenum.common.metrics_collector import MetricsCollector
from plenum.common.timer import TimerService
from plenum.server.catchup.ledger_leecher_service import LedgerLeecherService
from plenum.server.catchup.utils import CatchupDataProvider, LedgerCatchupComplete, NodeCatchupComplete
from stp_core.common.log import getlogger

logger = getlogger()


class NodeLeecherService:
    class State(Enum):
        Idle = 0
        SyncingAudit = 1
        SyncingPool = 2
        SyncingOthers = 3

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

        # TODO: Get rid of this, theoretically most ledgers can be synced in parallel
        self._current_ledger = None  # type: Optional[int]

        self._leecher_outbox, self._leecher_outbox_rx = create_direct_channel()
        Router(self._leecher_outbox_rx).add(LedgerCatchupComplete, self._on_ledger_catchup_complete)
        self._leecher_outbox_rx.subscribe(lambda msg: output.put_nowait(msg))

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

    def start(self, request_ledger_statuses: bool):
        for leecher in self._leechers.values():
            leecher.reset()

        self._state = self.State.SyncingAudit
        self._leechers[AUDIT_LEDGER_ID].start(request_ledger_statuses)

    def num_txns_caught_up_in_last_catchup(self) -> int:
        return sum(leecher.num_txns_caught_up for leecher in self._leechers.values())

    def _on_ledger_catchup_complete(self, msg: LedgerCatchupComplete):
        if self._state == self.State.SyncingAudit:
            self._on_audit_synced(msg)
        elif self._state == self.State.SyncingPool:
            self._on_pool_synced(msg)
        elif self._state == self.State.SyncingOthers:
            self._on_other_synced(msg)
        else:
            logger.warning("{} got unexpected catchup complete {} during idle state".format(self, msg))

    def _on_audit_synced(self, msg):
        if msg.ledger_id != AUDIT_LEDGER_ID:
            logger.warning("{} got unexpected catchup complete {} during syncing audit ledger".format(self, msg))
            return

        self._state = self.State.SyncingPool
        self._leechers[POOL_LEDGER_ID].start(request_ledger_statuses=True)

    def _on_pool_synced(self, msg):
        if msg.ledger_id != POOL_LEDGER_ID:
            logger.warning("{} got unexpected catchup complete {} during syncing pool ledger".format(self, msg))
            return

        self._state = self.State.SyncingOthers
        self._sync_next_ledger()

    def _on_other_synced(self, msg):
        if msg.ledger_id != self._current_ledger:
            logger.warning("{} got unexpected catchup complete {} during syncing ledger {}".
                           format(self, msg, self._current_ledger))
            return

        self._sync_next_ledger()

    def _sync_next_ledger(self):
        self._current_ledger = self._get_next_ledger(self._current_ledger)
        if self._current_ledger is not None:
            self._leechers[self._current_ledger].start(request_ledger_statuses=True)
        else:
            self._state = self.State.Idle
            self._output.put_nowait(NodeCatchupComplete())

    def _get_next_ledger(self, ledger_id: Optional[int]) -> Optional[int]:
        ledger_ids = list(self._leechers.keys())
        ledger_ids.remove(AUDIT_LEDGER_ID)
        ledger_ids.remove(POOL_LEDGER_ID)

        if len(ledger_ids) == 0:
            return None

        if ledger_id is None:
            return ledger_ids[0]

        next_index = ledger_ids.index(ledger_id) + 1
        if next_index == len(ledger_ids):
            return None

        return ledger_ids[next_index]
