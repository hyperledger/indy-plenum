from typing import Optional

import base58

from plenum.common.constants import POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID
from plenum.common.timer import QueueTimer
from plenum.common.util import get_utc_epoch
from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from plenum.server.consensus.primary_selector import RoundRobinConstantNodesPrimariesSelector
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.database_manager import DatabaseManager

from plenum.server.propagator import Requests

from plenum.server.node import Node

from plenum.common.metrics_collector import NullMetricsCollector
from plenum.test.testing_utils import FakeSomething
from stp_core.types import HA

from plenum.common.startable import Mode
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.common.ledger_manager import LedgerManager
from plenum.common.config_util import getConfigOnce

whitelist = ['but majority declared']

# TODO: Move these helper classes to some common place - there were used not only in this test


class FakeLedger:
    def __init__(self, ledger_id, size):
        self._size = size
        self.root_hash = base58.b58encode(str(ledger_id).encode() * 32).decode("utf-8")
        self.hasher = None

    def __len__(self):
        return self._size


# Question: Why doesn't this subclass Node.
class FakeNode:
    ledger_ids = [POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID]

    def __init__(self, tmpdir, config=None):
        node_names = ['Node1', 'Node2', 'Node3', 'Node4']
        self.basedirpath = tmpdir
        self.name = node_names[0]
        self.viewNo = 0
        self.db_manager = DatabaseManager()
        self.timer = QueueTimer()
        self.f = 1
        self.replicas = dict()
        self.requests = Requests()
        self.rank = None
        self.allNodeNames = node_names
        self.nodeReg = {
            name: HA("127.0.0.1", 0) for name in self.allNodeNames
        }
        self.nodeIds = []
        self.totalNodes = len(self.allNodeNames)
        self.poolManager = FakeSomething(node_names_ordered_by_rank=lambda: node_names)
        self.mode = Mode.starting
        self.monitor = FakeSomething(isMasterDegraded=lambda: False)
        self.config = config or getConfigOnce()
        self.nodeStatusDB = None
        self.quorums = Quorums(self.totalNodes)
        self.nodestack = FakeSomething(connecteds=set(self.allNodeNames))
        self.write_manager = FakeSomething(node_reg_handler=NodeRegHandler(self.db_manager))
        self.primaries_selector = RoundRobinConstantNodesPrimariesSelector(node_names)
        self.replicas = {
            0: Replica(node=self, instId=0, isMaster=True, config=self.config),
            1: Replica(node=self, instId=1, isMaster=False, config=self.config),
            2: Replica(node=self, instId=2, isMaster=False, config=self.config)
        }
        self.requiredNumberOfInstances = 2
        self._found = False
        self.ledgerManager = LedgerManager(self)
        ledger0 = FakeLedger(0, 10)
        ledger1 = FakeLedger(1, 5)
        self.ledgerManager.addLedger(0, ledger0)
        self.ledgerManager.addLedger(1, ledger1)
        self.quorums = Quorums(self.totalNodes)
        self.metrics = NullMetricsCollector()

        # For catchup testing
        self.view_change_in_progress = False
        self.ledgerManager.last_caught_up_3PC = (0, 0)
        self.master_last_ordered_3PC = (0, 0)
        self.seqNoDB = {}

        # callbacks
        self.onBatchCreated = lambda self, *args, **kwargs: True

    @property
    def ledger_summary(self):
        return [li.ledger_summary for li in
                self.ledgerManager.ledgerRegistry.values()]

    def get_name_by_rank(self, name, node_reg, node_ids):
        # This is used only for getting name of next primary, so
        # it just returns a constant
        return 'Node2'

    def primary_selected(self, instance_id):
        self._found = True

    def is_primary_found(self):
        return self._found

    @property
    def master_primary_name(self) -> Optional[str]:
        nm = self.replicas[0].primaryName
        if nm:
            return replica_name_to_node_name(nm)

    @property
    def master_replica(self):
        return self.replicas[0]

    @property
    def is_synced(self):
        return self.mode >= Mode.synced

    def on_view_change_start(self):
        pass

    def start_catchup(self):
        pass

    def allLedgersCaughtUp(self):
        Node.allLedgersCaughtUp(self)

    def _clean_non_forwarded_ordered(self):
        return Node._clean_non_forwarded_ordered(self)

    def num_txns_caught_up_in_last_catchup(self):
        return Node.num_txns_caught_up_in_last_catchup(self)

    def set_view_change_status(self, value):
        return Node.set_view_change_status(self, value)

    def mark_request_as_executed(self, request):
        Node.mark_request_as_executed(self, request)

    def _clean_req_from_verified(self, request):
        pass

    def doneProcessingReq(self, key):
        pass

    def is_catchup_needed(self):
        return False

    def no_more_catchups_needed(self):
        pass

    def utc_epoch(self):
        return get_utc_epoch()

    def get_validators(self):
        return []

    def get_primaries_for_current_view(self):
        # This is used only for getting name of next primary, so
        # it just returns a constant
        return ['Node2', 'Node3']
