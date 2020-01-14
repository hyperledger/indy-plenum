from plenum.server.view_change.view_changer import ViewChanger, ViewChangerDataProvider


class ViewChangerNodeDataProvider(ViewChangerDataProvider):
    def __init__(self, node):
        self._node = node

    def view_no(self):
        return self._node.master_replica.viewNo

    def view_change_in_progress(self):
        return self._node.master_replica._consensus_data.waiting_for_new_view


def create_view_changer(node, vchCls=ViewChanger):
    return vchCls(ViewChangerNodeDataProvider(node), node.timer)
