import logging
from typing import Optional, Set


from plenum.common.config_util import getConfig
from plenum.server.quorums import Quorums
from plenum.server.view_change.view_changer import ViewChangerDataProvider, ViewChanger
from stp_core.common.log import getlogger

config = getConfig()
logger = getlogger()


class ViewChangerNodeModelDataProvider(ViewChangerDataProvider):
    def __init__(self, node):
        self._node = node
        self._has_primary = True

    def name(self) -> str:
        return self._node.name

    def config(self) -> object:
        return config

    def quorums(self) -> Quorums:
        return self._node._quorum

    def has_primary(self) -> bool:
        return self._has_primary

    def is_primary(self) -> Optional[bool]:
        if self._has_primary:
            return self._node.is_primary

    def is_primary_disconnected(self) -> bool:
        return self._node.is_primary_disconnected

    def is_master_degraded(self) -> bool:
        return self._node._corrupted_name == self._node.primary_name

    def pretty_metrics(self) -> str:
        return ""

    def state_freshness(self) -> float:
        return 0

    def connected_nodes(self) -> Set[str]:
        return set(self._node.connected_nodes)

    def notify_view_change_start(self):
        self._has_primary = False

    def select_primaries(self, node_reg):
        self._has_primary = True

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        logMethod("Discarding {} because of {}".format(msg, reason))

    @property
    def node_status_db(self):
        return None


def create_view_changer(node):
    return ViewChanger(ViewChangerNodeModelDataProvider(node), node._timer)
