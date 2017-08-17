from typing import Callable, Any, List, Dict

from plenum import config
from plenum.common.batched import Batched, logger
from plenum.common.config_util import getConfig
from plenum.common.message_processor import MessageProcessor
from stp_core.common.constants import CONNECTION_PREFIX
from stp_raet.rstack import SimpleRStack, KITRStack
from stp_core.types import HA
from stp_zmq.kit_zstack import KITZStack
from stp_zmq.simple_zstack import SimpleZStack


class ClientZStack(SimpleZStack, MessageProcessor):
    def __init__(self, stackParams: dict, msgHandler: Callable, seed=None,
                 config=None, msgRejectHandler=None):
        config = config or getConfig()
        SimpleZStack.__init__(
            self,
            stackParams,
            msgHandler,
            seed=seed,
            onlyListener=True,
            config=config,
            msgRejectHandler=msgRejectHandler)
        MessageProcessor.__init__(self, allowDictOnly=False)
        self.connectedClients = set()

    def serviceClientStack(self):
        newClients = self.connecteds - self.connectedClients
        self.connectedClients = self.connecteds
        return newClients

    def newClientsConnected(self, newClients):
        raise NotImplementedError("{} must implement this method".format(self))

    def transmitToClient(self, msg: Any, remoteName: str):
        """
        Transmit the specified message to the remote client specified by `remoteName`.

        :param msg: a message
        :param remoteName: the name of the remote
        """
        # At this time, nodes are not signing messages to clients, beyond what
        # happens inherently with RAET
        payload = self.prepForSending(msg)
        try:
            if isinstance(remoteName, str):
                remoteName = remoteName.encode()
            self.send(payload, remoteName)
        except Exception as ex:
            # TODO: This should not be an error since the client might not have
            # sent the request to all nodes but only some nodes and other
            # nodes might have got this request through PROPAGATE and thus
            # might not have connection with the client.
            logger.error(
                "{}{} unable to send message {} to client {}; Exception: {}" .format(
                    CONNECTION_PREFIX, self, msg, remoteName, ex.__repr__()))

    def transmitToClients(self, msg: Any, remoteNames: List[str]):
        # TODO: Handle `remoteNames`
        for nm in self.peersWithoutRemotes:
            self.transmitToClient(msg, nm)


class NodeZStack(Batched, KITZStack):
    def __init__(self, stackParams: dict, msgHandler: Callable,
                 registry: Dict[str, HA], seed=None, sighex: str=None,
                 config=None):
        config = config or getConfig()
        Batched.__init__(self, config=config)
        KITZStack.__init__(self, stackParams, msgHandler, registry=registry,
                           seed=seed, sighex=sighex, config=config)
        MessageProcessor.__init__(self, allowDictOnly=False)

    # TODO: Reconsider defaulting `reSetupAuth` to True.
    def start(self, restricted=None, reSetupAuth=True):
        KITZStack.start(self, restricted=restricted, reSetupAuth=reSetupAuth)
        # Calling service lifecycle to allow creation of remotes
        # that this stack needs to connect to
        # self.serviceLifecycle()
        logger.info("{}{} listening for other nodes at {}:{}".
                    format(CONNECTION_PREFIX, self, *self.ha),
                    extra={"tags": ["node-listening"]})


class ClientRStack(SimpleRStack, MessageProcessor):
    def __init__(self, stackParams: dict, msgHandler: Callable, seed=None):
        # The client stack needs to be mutable unless we explicitly decide
        # not to
        stackParams["mutable"] = stackParams.get("mutable", True)
        stackParams["messageTimeout"] = config.RAETMessageTimeout
        SimpleRStack.__init__(self, stackParams, msgHandler)
        MessageProcessor.__init__(self, allowDictOnly=True)
        self.connectedClients = set()

    def serviceClientStack(self):
        newClients = self.connecteds - self.connectedClients
        self.connectedClients = self.connecteds
        return newClients

    def newClientsConnected(self, newClients):
        raise NotImplementedError("{} must implement this method".format(self))

    def transmitToClient(self, msg: Any, remoteName: str):
        """
        Transmit the specified message to the remote client specified by `remoteName`.

        :param msg: a message
        :param remoteName: the name of the remote
        """
        # At this time, nodes are not signing messages to clients, beyond what
        # happens inherently with RAET
        payload = self.prepForSending(msg)
        try:
            self.send(payload, remoteName)
        except Exception as ex:
            # TODO: This should not be an error since the client might not have
            # sent the request to all nodes but only some nodes and other
            # nodes might have got this request through PROPAGATE and thus
            # might not have connection with the client.
            logger.error(
                "{}{} unable to send message {} to client {}; Exception: {}" .format(
                    CONNECTION_PREFIX, self, msg, remoteName, ex.__repr__()))

    def transmitToClients(self, msg: Any, remoteNames: List[str]):
        for nm in remoteNames:
            self.transmitToClient(msg, nm)


class NodeRStack(Batched, KITRStack):
    def __init__(self, stackParams: dict, msgHandler: Callable,
                 registry: Dict[str, HA], seed=None, sighex: str=None):
        Batched.__init__(self)
        # TODO: Just to get around the restriction of port numbers changed on
        # Azure. Remove this soon to relax port numbers only but not IP.
        stackParams["mutable"] = stackParams.get("mutable", True)
        stackParams["messageTimeout"] = config.RAETMessageTimeout
        KITRStack.__init__(self, stackParams, msgHandler, registry, sighex)
        MessageProcessor.__init__(self, allowDictOnly=True)

    def start(self):
        KITRStack.start(self)
        logger.info("{}{} listening for other nodes at {}:{}".
                    format(CONNECTION_PREFIX, self, *self.ha),
                    extra={"tags": ["node-listening"]})


nodeStackClass = NodeZStack if config.UseZStack else NodeRStack
clientStackClass = ClientZStack if config.UseZStack else ClientRStack
