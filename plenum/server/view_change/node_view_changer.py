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

    def propose_view_change(self, suspicion: Suspicion):
        self._node.master_replica.internal_bus.send(VoteForViewChange(suspicion))

    def view_no(self):
        return self._node.master_replica.viewNo

    def view_change_in_progress(self):
        return self._node.master_replica._consensus_data.waiting_for_new_view


def create_view_changer(node, vchCls=ViewChanger):
    return vchCls(ViewChangerNodeDataProvider(node), node.timer)
