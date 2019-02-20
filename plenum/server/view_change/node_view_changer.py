import logging
from typing import Tuple, List, Set

from plenum.common.startable import Mode
from plenum.common.timer import QueueTimer
from plenum.server.quorums import Quorums
from plenum.server.view_change.pre_view_change_strategies import preVCStrategies
from plenum.server.view_change.view_changer import ViewChanger, ViewChangerDataProvider
from storage.kv_store import KeyValueStorage


class ViewChangerNodeDataProvider(ViewChangerDataProvider):
    def __init__(self, node):
        self._node = node

    def name(self) -> str:
        return self._node.name

    def config(self) -> object:
        return self._node.config

    def quorums(self) -> Quorums:
        return self._node.quorums

    def has_pool_ledger(self) -> bool:
        return self._node.poolLedger is not None

    def ledger_summary(self) -> List[Tuple[int, int, str]]:
        return self._node.ledger_summary

    def node_registry(self, size):
        return self._node.poolManager.getNodeRegistry(size)

    def is_node_synced(self) -> bool:
        return self._node.is_synced

    def node_mode(self) -> Mode:
        return self._node.mode

    def next_primary_name(self) -> str:
        return self._node.elector._next_primary_node_name_for_master()

    def current_primary_name(self) -> str:
        return self._node.master_primary_name

    def has_primary(self) -> bool:
        return self._node.master_replica.hasPrimary

    def is_primary(self):
        return self._node.master_replica.isPrimary

    def is_primary_disconnected(self) -> bool:
        return \
            self._node.primaries_disconnection_times[self._node.master_replica.instId] \
            and self._node.master_primary_name \
            and self._node.master_primary_name not in self._node.nodestack.conns

    def is_master_degraded(self) -> bool:
        return self._node.monitor.isMasterDegraded()

    def pretty_metrics(self) -> str:
        return self._node.monitor.prettymetrics

    def state_freshness(self) -> float:
        replica = self._node.master_replica
        timestamps = replica.get_ledgers_last_update_time().values()
        oldest_timestamp = min(timestamps)
        return replica.get_time_for_3pc_batch() - oldest_timestamp

    def connected_nodes(self) -> Set[str]:
        return self._node.nodestack.connecteds

    def notify_view_change_start(self):
        self._node.on_view_change_start()

    def notify_view_change_complete(self):
        self._node.on_view_change_complete()

    def notify_initial_propose_view_change(self):
        self._node.schedule_initial_propose_view_change()

    def start_catchup(self):
        self._node.start_catchup()

    def restore_backup_replicas(self):
        self._node.backup_instance_faulty_processor.restore_replicas()

    def select_primaries(self, node_reg):
        self._node.select_primaries(node_reg)

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        self._node.discard(msg, reason, logMethod, cliOutput)

    @property
    def node_status_db(self) -> KeyValueStorage:
        return self._node.nodeStatusDB


def create_view_changer(node, vchCls=ViewChanger):
    vc = vchCls(ViewChangerNodeDataProvider(node), node.timer)

    if hasattr(node.config, 'PRE_VC_STRATEGY'):
        vc.pre_vc_strategy = preVCStrategies.get(node.config.PRE_VC_STRATEGY)(vc, node)

    return vc
