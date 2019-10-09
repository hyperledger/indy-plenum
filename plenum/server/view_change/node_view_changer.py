import logging
from typing import Set

from plenum.common.messages.internal_messages import NeedViewChange
from plenum.common.startable import Mode
from plenum.server.quorums import Quorums
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

    def node_mode(self) -> Mode:
        return self._node.mode

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

    def select_primaries(self):
        self._node.select_primaries()

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        self._node.discard(msg, reason, logMethod, cliOutput)

    @property
    def node_status_db(self) -> KeyValueStorage:
        return self._node.nodeStatusDB

    def schedule_resend_inst_chng(self):
        self._node.schedule_view_change_completion_check(self._node.config.INSTANCE_CHANGE_RESEND_TIMEOUT)

    def start_view_change(self, proposed_view_no: int):
        for replica in self._node.replicas.values():
            replica.internal_bus.send(NeedViewChange(view_no=proposed_view_no))

    def view_no(self):
        return self._node.master_replica.viewNo

    def view_change_in_progress(self):
        return self._node.master_replica._consensus_data.waiting_for_new_view


def create_view_changer(node, vchCls=ViewChanger):
    return vchCls(ViewChangerNodeDataProvider(node), node.timer)
