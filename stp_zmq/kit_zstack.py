from plenum.common.config_util import get_global_config_else_read_config
from plenum.common.metrics_collector import NullMetricsCollector
from plenum.recorder.simple_zstack_with_recorder import SimpleZStackWithRecorder
from plenum.recorder.simple_zstack_with_silencer import SimpleZStackWithSilencer
from stp_core.common.constants import CONNECTION_PREFIX
from stp_core.network.exceptions import PublicKeyNotFoundOnDisk, VerKeyNotFoundOnDisk
from stp_core.network.keep_in_touch import KITNetworkInterface
from stp_zmq.simple_zstack import SimpleZStack
from typing import Dict, Callable, Optional
from stp_core.types import HA
import time
from stp_core.common.log import getlogger
from stp_zmq.zstack import Quota

logger = getlogger()


conf_ = get_global_config_else_read_config()

if conf_.STACK_COMPANION == 1:
    simple_zstack_class = SimpleZStackWithRecorder
elif conf_.STACK_COMPANION == 2:
    simple_zstack_class = SimpleZStackWithSilencer
else:
    simple_zstack_class = SimpleZStack


class KITZStack(simple_zstack_class, KITNetworkInterface):
    # ZStack which maintains connections mentioned in its registry

    def __init__(self,
                 stackParams: dict,
                 msgHandler: Callable,
                 registry: Dict[str, HA],
                 seed=None,
                 sighex: str = None,
                 config=None,
                 msgRejectHandler=None,
                 metrics=NullMetricsCollector(),
                 mt_incoming_size=None,
                 mt_outgoing_size=None):

        KITNetworkInterface.__init__(self, registry=registry)

        simple_zstack_class.__init__(self, stackParams, msgHandler,
                                     seed=seed, sighex=sighex, config=config,
                                     msgRejectHandler=msgRejectHandler,
                                     metrics=metrics,
                                     mt_incoming_size=mt_incoming_size,
                                     mt_outgoing_size=mt_outgoing_size)

        self._retry_connect = {}

    def maintainConnections(self, force=False):
        """
        Ensure appropriate connections.

        """
        now = time.perf_counter()
        if now < self.nextCheck and not force:
            return False
        self.nextCheck = now + (self.config.RETRY_TIMEOUT_NOT_RESTRICTED
                                if self.isKeySharing
                                else self.config.RETRY_TIMEOUT_RESTRICTED)
        missing = self.connectToMissing()
        self.retryDisconnected(exclude=missing)
        logger.trace("{} next check for retries in {:.2f} seconds"
                     .format(self, self.nextCheck - now))
        return True

    def reconcileNodeReg(self) -> set:
        """
        Check whether registry contains some addresses
        that were never connected to
        :return:
        """

        matches = set()
        for name, remote in self.remotes.items():
            if name not in self.registry:
                continue
            if self.sameAddr(remote.ha, self.registry[name]):
                matches.add(name)
                logger.debug("{} matched remote {} {}".
                             format(self, remote.uid, remote.ha))
        return self.registry.keys() - matches - {self.name}

    def retryDisconnected(self, exclude=None):
        if not self.config.RETRY_CONNECT:
            return
        exclude = exclude or {}
        for name, remote in self.remotes.items():
            if name in exclude or remote.isConnected:
                if name in self._retry_connect:
                    self._retry_connect.pop(name, None)
                continue

            if not self.config.RETRY_SOCKET_RECONNECT:
                self.sendPingPong(remote, is_ping=True)
                continue

            if name not in self._retry_connect:
                self._retry_connect[name] = 0

            if not remote.socket or self._retry_connect[name] >= \
                    self.config.MAX_RECONNECT_RETRY_ON_SAME_SOCKET:
                self._retry_connect.pop(name, None)
                self.reconnectRemote(remote)
            else:
                self._retry_connect[name] += 1
                self.sendPingPong(remote, is_ping=True)

    def connectToMissing(self) -> set:
        """
        Try to connect to the missing nodes
        """

        missing = self.reconcileNodeReg()
        if not missing:
            return missing

        logger.info("{}{} found the following missing connections: {}".
                    format(CONNECTION_PREFIX, self, ", ".join(missing)))

        for name in missing:
            try:
                self.connect(name, ha=self.registry[name])
            except (ValueError, KeyError, PublicKeyNotFoundOnDisk, VerKeyNotFoundOnDisk) as ex:
                logger.warning('{}{} cannot connect to {} due to {}'.
                               format(CONNECTION_PREFIX, self, name, ex))
        return missing

    async def service(self, limit=None, quota: Optional[Quota] = None):
        c = await super().service(limit, quota)
        return c
