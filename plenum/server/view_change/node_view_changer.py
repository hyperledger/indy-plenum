from plenum.common.messages.internal_messages import VoteForViewChange
from plenum.common.startable import Mode
from plenum.server.quorums import Quorums
from plenum.server.suspicion_codes import Suspicion
from plenum.server.view_change.view_changer import ViewChanger, ViewChangerDataProvider


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

    def is_primary_disconnected(self) -> bool:
        return \
            self._node.primaries_disconnection_times[self._node.master_replica.instId] \
            and self._node.master_primary_name \
            and self._node.master_primary_name not in self._node.nodestack.conns

    def state_freshness(self) -> float:
        replica = self._node.master_replica
        timestamps = replica.get_ledgers_last_update_time().values()
        oldest_timestamp = min(timestamps)
        return replica.get_time_for_3pc_batch() - oldest_timestamp

    def propose_view_change(self, suspicion: Suspicion):
        self._node.master_replica.internal_bus.send(VoteForViewChange(suspicion))

    def view_no(self):
        return self._node.master_replica.viewNo

    def view_change_in_progress(self):
        return self._node.master_replica._consensus_data.waiting_for_new_view


def create_view_changer(node, vchCls=ViewChanger):
    return vchCls(ViewChangerNodeDataProvider(node), node.timer)
