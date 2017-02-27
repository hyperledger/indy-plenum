import sys
import time
from collections import Callable
from collections import deque
from typing import Any, Set, Optional, List, Iterable
from typing import Dict
from typing import Tuple

from raet.raeting import AutoMode
from raet.road.estating import RemoteEstate
from raet.road.keeping import RoadKeep
from raet.road.stacking import RoadStack
from raet.road.transacting import Joiner, Allower, Messenger

from plenum.common.crypto import getEd25519AndCurve25519Keys, \
    ed25519SkToCurve25519
from plenum.common.exceptions import RemoteNotFound
from plenum.common.log import getlogger
from plenum.common.ratchet import Ratchet
from plenum.common.signer import Signer
from plenum.common.types import Batch, TaggedTupleBase, HA
from plenum.common.request import Request
from plenum.common.util import distributedConnectionMap, \
    MessageProcessor, checkPortAvailable
from plenum.common.config_util import getConfig
from plenum.common.error import error

logger = getlogger()

# this overrides the defaults
Joiner.RedoTimeoutMin = 1.0
Joiner.RedoTimeoutMax = 10.0

Allower.RedoTimeoutMin = 1.0
Allower.RedoTimeoutMax = 10.0

Messenger.RedoTimeoutMin = 1.0
Messenger.RedoTimeoutMax = 10.0


class Stack(RoadStack):
    def __init__(self, *args, **kwargs):
        checkPortAvailable(kwargs['ha'])
        basedirpath = kwargs.get('basedirpath')
        keep = RoadKeep(basedirpath=basedirpath,
                        stackname=kwargs['name'],
                        auto=kwargs.get('auto'),
                        baseroledirpath=basedirpath)  # type: RoadKeep
        kwargs['keep'] = keep
        localRoleData = keep.loadLocalRoleData()

        sighex = kwargs.pop('sighex', None) or localRoleData['sighex']
        if not sighex:
            (sighex, _), (prihex, _) = getEd25519AndCurve25519Keys()
        else:
            prihex = ed25519SkToCurve25519(sighex, toHex=True)
        kwargs['sigkey'] = sighex
        kwargs['prikey'] = prihex
        self.msgHandler = kwargs.pop('msgHandler', None)  # type: Callable
        super().__init__(*args, **kwargs)
        if self.ha[1] != kwargs['ha'].port:
            error("the stack port number has changed, likely due to "
                  "information in the keep. {} passed {}, actual {}".
                  format(kwargs['name'], kwargs['ha'].port, self.ha[1]))
        self.created = time.perf_counter()
        self.coro = None
        config = getConfig()
        try:
            self.messageTimeout = config.RAETMessageTimeout
        except AttributeError:
            # if no timeout is set then message will never timeout
            self.messageTimeout = 0

    def __repr__(self):
        return self.name

    def start(self):
        if not self.opened:
            self.open()
        logger.info("stack {} starting at {} in {} mode"
                    .format(self, self.ha, self.keep.auto.name),
                    extra={"cli": False})
        self.coro = self._raetcoro()

    def stop(self):
        if self.opened:
            self.close()
        self.coro = None
        logger.info("stack {} stopped".format(self.name), extra={"cli": False})

    async def service(self, limit=None) -> int:
        """
        Service `limit` number of received messages in this stack.

        :param limit: the maximum number of messages to be processed. If None,
        processes all of the messages in rxMsgs.
        :return: the number of messages processed.
        """
        pracLimit = limit if limit else sys.maxsize
        if self.coro:
            x = next(self.coro)
            if x > 0:
                for x in range(pracLimit):
                    try:
                        self.msgHandler(self.rxMsgs.popleft())
                    except IndexError:
                        break
            return x
        else:
            logger.debug("{} is stopped".format(self))
            return 0

    @property
    def age(self):
        """
        Returns the time elapsed since this stack was created
        """
        return time.perf_counter() - self.created

    def _raetcoro(self):
        """
        Generator to service all messages.
        Yields the length of rxMsgs queue of this stack.
        """
        while True:
            try:
                self._serviceStack(self.age)
                l = len(self.rxMsgs)
            except Exception as ex:
                if isinstance(ex, OSError) and \
                        len(ex.args) > 0 and \
                        ex.args[0] == 22:
                    logger.error("Error servicing stack {}: {}. This could be "
                                 "due to binding to an internal network "
                                 "and trying to route to an external one.".
                                 format(self.name, ex), extra={'cli': 'WARNING'})
                else:
                    logger.error("Error servicing stack {}: {} {}".
                                 format(self.name, ex, ex.args),
                                 extra={'cli': 'WARNING'})
                l = 0
            yield l

    def _serviceStack(self, age):
        """
        Update stacks clock and service all tx and rx messages.

        :param age: update timestamp of this RoadStack to this value
        """
        self.updateStamp(age)
        self.serviceAll()

    def updateStamp(self, age=None):
        """
        Change the timestamp of this stack's raet store.

        :param age: the timestamp will be set to this value
        """
        self.store.changeStamp(age if age else self.age)

    @property
    def opened(self):
        return self.server.opened

    def open(self):
        """
        Open the UDP socket of this stack's server.
        """
        self.server.open()  # close the UDP socket

    def close(self):
        """
        Close the UDP socket of this stack's server.
        """
        self.server.close()  # close the UDP socket

    # TODO: Does this serve the same purpose as `conns`, if yes then remove
    @property
    def connecteds(self) -> Set[str]:
        """
        Return the names of the nodes this node is connected to.
        """
        return {r.name for r in self.remotes.values()
                if self.isRemoteConnected(r)}

    @staticmethod
    def isRemoteConnected(r: RemoteEstate) -> bool:
        """
        A node is considered to be connected if it is joined, allowed and alived.

        :param r: the remote to check
        """
        return r.joined and r.allowed and r.alived

    def isConnectedTo(self, name: str=None, ha: Tuple=None):
        assert (name, ha).count(None) == 1, "One and only one of name or ha " \
                                            "should be passed. Passed " \
                                            "name: {}, ha: {}".format(name, ha)
        try:
            remote = self.getRemote(name, ha)
        except RemoteNotFound:
            return False
        return self.isRemoteConnected(remote)

    def getRemote(self, name: str=None, ha: Tuple=None) -> RemoteEstate:
        """
        Find the remote by name or ha.

        :param name: the name of the remote to find
        :param ha: host address pair the remote to find
        :raises: RemoteNotFound
        """
        assert (name, ha).count(None) == 1, "One and only one of name or ha " \
                                            "should be passed. Passed " \
                                            "name: {}, ha: {}".format(name, ha)
        remote = self.findInRemotesByName(name) if name else \
            self.findInRemotesByHA(ha)
        if not remote:
            raise RemoteNotFound(name or ha)
        return remote

    def findInRemotesByHA(self, remoteHa):
        remotes = [r for r in self.remotes.values()
                   if r.ha == remoteHa]
        assert len(remotes) <= 1, "Found remotes {}: {}".\
            format(len(remotes), [(r.name, r.ha) for r in remotes])
        if remotes:
            return remotes[0]
        return None

    def findInRemotesByName(self, name: str) -> RemoteEstate:
        """
        Find the remote by name.

        :param name: the name of the remote to find
        :raises: RemoteNotFound
        """
        try:
            return next(r for r in self.remotes.values()
                        if r.name == name)
        except StopIteration:
            return None

    def removeRemoteByName(self, name: str) -> int:
        """
        Remove the remote by name.

        :param name: the name of the remote to remove
        :raises: RemoteNotFound
        """
        remote = self.getRemote(name)
        rid = remote.uid
        self.removeRemote(remote)
        return rid

    def send(self, msg: Any, remoteName: str):
        """
        Transmit the specified message to the remote specified by `remoteName`.

        :param msg: a message
        :param remoteName: the name of the remote
        """
        rid = self.getRemote(remoteName).uid
        # Setting timeout to never expire
        self.transmit(msg, rid, timeout=self.messageTimeout)


class SimpleStack(Stack):
    localips = ['127.0.0.1', '0.0.0.0']

    def __init__(self, stackParams: Dict, msgHandler: Callable, sighex: str=None):
        self.stackParams = stackParams
        self.msgHandler = msgHandler
        self._conns = set()  # type: Set[str]
        super().__init__(**stackParams, msgHandler=self.msgHandler, sighex=sighex)

    @property
    def isKeySharing(self):
        return self.keep.auto != AutoMode.never

    @property
    def conns(self) -> Set[str]:
        """
        Get the connections of this node.

        :return: set of names of the connected nodes
        """
        return self._conns

    @conns.setter
    def conns(self, value: Set[str]) -> None:
        """
        Updates the connection count of this node if not already done.
        """
        if not self._conns == value:
            old = self._conns
            self._conns = value
            ins = value - old
            outs = old - value
            logger.debug("{}'s connections changed from {} to {}".format(self,
                                                                         old,
                                                                         value))
            self._connsChanged(ins, outs)

    def checkConns(self):
        """
        Evaluate the connected nodes
        """
        self.conns = self.connecteds

    def _connsChanged(self, ins: Set[str], outs: Set[str]) -> None:
        """
        A series of operations to perform once a connection count has changed.

        - Set f to max number of failures this system can handle.
        - Set status to one of started, started_hungry or starting depending on
            the number of protocol instances.
        - Check protocol instances. See `checkProtocolInstaces()`

        :param ins: new nodes connected
        :param outs: nodes no longer connected
        """
        for o in outs:
            logger.info("{} disconnected from {}".format(self, o),
                        extra={"cli": "IMPORTANT",
                               "tags": ["connected"]})
        for i in ins:
            logger.info("{} now connected to {}".format(self, i),
                        extra={"cli": "IMPORTANT",
                               "tags": ["connected"]})

            # remove remotes for same ha when a connection is made
            remote = self.getRemote(i)
            others = [r for r in self.remotes.values()
                      if r.ha == remote.ha and r.name != i]
            for o in others:
                logger.debug("{} removing other remote".format(self))
                self.removeRemote(o)

        self.onConnsChanged(ins, outs)

    def onConnsChanged(self, ins: Set[str], outs: Set[str]):
        """
        Subclasses can override
        """
        pass

    def start(self):
        super().start()
        # super().__init__(**self.stackParams, msgHandler=self.msgHandler)

    def sign(self, msg: Dict, signer: Signer) -> Dict:
        """
        No signing is implemented. Returns the msg as it is.
        An overriding class can define the signing implementation

        :param msg: the message to sign
        """
        return msg  # don't sign by default

    def prepForSending(self, msg: Dict, signer: Signer = None) -> Dict:
        """
        Return a dictionary form of the message

        :param msg: the message to be sent
        :raises: ValueError if msg cannot be converted to an appropriate format
            for transmission
        """
        if isinstance(msg, TaggedTupleBase):
            tmsg = msg.melted()
        elif isinstance(msg, Request):
            tmsg = msg.__getstate__()
        elif hasattr(msg, "_asdict"):
            tmsg = dict(msg._asdict())
        elif hasattr(msg, "__dict__"):
            tmsg = dict(msg.__dict__)
        else:
            raise ValueError("Message cannot be converted to an appropriate "
                             "format for transmission")
        if signer:
            return self.sign(tmsg, signer)
        return tmsg

    def sameAddr(self, ha, ha2) -> bool:
        """
        Check whether the two arguments correspond to the same address
        """
        if ha == ha2:
            return True
        elif ha[1] != ha2[1]:
            return False
        else:
            return ha[0] in self.localips and ha2[0] in self.localips


class KITStack(SimpleStack):
    # Keep In Touch Stack. Stack which maintains connections mentioned in
    # its registry
    def __init__(self, stackParams: dict, msgHandler: Callable,
                 registry: Dict[str, HA], sighex: str=None):
        super().__init__(stackParams, msgHandler, sighex)
        self.registry = registry
        # self.bootstrapped = False

        self.lastcheck = {}  # type: Dict[int, Tuple[int, float]]
        self.ratchet = Ratchet(a=8, b=0.198, c=-4, base=8, peak=3600)

        # holds the last time we checked remotes
        self.nextCheck = 0

        # courteous bi-directional joins
        self.connectNicelyUntil = None

        self.reconnectToMissingIn = 6
        self.reconnectToDisconnectedIn = 6

    def start(self):
        super().start()
        if self.name in self.registry:
            # remove this node's registration from the  Registry
            # (no need to connect to itself)
            del self.registry[self.name]

    async def serviceLifecycle(self) -> None:
        """
        Async function that does the following activities if the node is going:
        (See `Status.going`)

        - check connections (See `checkConns`)
        - maintain connections (See `maintainConnections`)
        """
        self.checkConns()
        self.maintainConnections()

    def connect(self, name, rid: Optional[int]=None) -> Optional[int]:
        """
        Connect to the node specified by name.

        :param name: name of the node to connect to
        :type name: str or (HA, tuple)
        :return: the uid of the remote estate, or None if a connect is not
            attempted
        """
        # if not self.isKeySharing:
        #     logger.debug("{} skipping join with {} because not key sharing".
        #                   format(self, name))
        #     return None
        if rid:
            remote = self.remotes[rid]
        else:
            if isinstance(name, (HA, tuple)):
                node_ha = name
            elif isinstance(name, str):
                node_ha = self.registry[name]
            else:
                raise AttributeError()

            remote = RemoteEstate(stack=self,
                                  ha=node_ha)
            self.addRemote(remote)
        # updates the store time so the join timer is accurate
        self.updateStamp()
        self.join(uid=remote.uid, cascade=True, timeout=30)
        logger.info("{} looking for {} at {}:{}".
                    format(self, name or remote.name, *remote.ha),
                    extra={"cli": "PLAIN", "tags": ["node-looking"]})
        return remote.uid

    @property
    def notConnectedNodes(self) -> Set[str]:
        """
        Returns the names of nodes in the registry this node is NOT connected
        to.
        """
        return set(self.registry.keys()) - self.conns

    def maintainConnections(self, force=False):
        """
        Ensure appropriate connections.

        """
        cur = time.perf_counter()
        if cur > self.nextCheck or force:

            self.nextCheck = cur + (6 if self.isKeySharing else 15)
            # check again in 15 seconds,
            # unless sooner because of retries below

            conns, disconns = self.remotesByConnected()

            for disconn in disconns:
                self.handleDisconnectedRemote(cur, disconn)

            # remove items that have been connected
            for connected in conns:
                self.lastcheck.pop(connected.uid, None)

            self.connectToMissing(cur)

            logger.debug("{} next check for retries in {:.2f} seconds".
                         format(self, self.nextCheck - cur))
            return True
        return False

    def connectToMissing(self, currentTime):
        """
        Try to connect to the missing node within the time specified by
        `reconnectToMissingIn`

        :param currentTime: the current time
        """
        missing = self.reconcileNodeReg()
        if missing:
            logger.debug("{} found the following missing connections: {}".
                         format(self, ", ".join(missing)))
            if self.connectNicelyUntil is None:
                self.connectNicelyUntil = \
                    currentTime + self.reconnectToMissingIn
            if currentTime <= self.connectNicelyUntil:
                names = list(self.registry.keys())
                names.append(self.name)
                nices = set(distributedConnectionMap(names)[self.name])
                for name in nices:
                    logger.debug("{} being nice and waiting for {} to join".
                                 format(self, name))
                missing = missing.difference(nices)

            for name in missing:
                self.connect(name)

    def handleDisconnectedRemote(self, cur, disconn):
        """

        :param disconn: disconnected remote
        """

        # if disconn.main:
        #     logger.trace("{} remote {} is main, so skipping".
        #                  format(self, disconn.uid))
        #     return

        logger.trace("{} handling disconnected remote {}".format(self, disconn))

        if disconn.joinInProcess():
            logger.trace("{} join already in process, so "
                         "waiting to check for reconnects".
                         format(self))
            self.nextCheck = min(self.nextCheck,
                                 cur + self.reconnectToDisconnectedIn)
            return

        if disconn.allowInProcess():
            logger.trace("{} allow already in process, so "
                         "waiting to check for reconnects".
                         format(self))
            self.nextCheck = min(self.nextCheck,
                                 cur + self.reconnectToDisconnectedIn)
            return

        if disconn.name not in self.registry:
            # TODO this is almost identical to line 615; make sure we refactor
            regName = self.findInNodeRegByHA(disconn.ha)
            if regName:
                logger.debug("{} forgiving name mismatch for {} with same "
                             "ha {} using another name {}".
                             format(self, regName, disconn.ha, disconn.name))
            else:
                logger.debug("{} skipping reconnect on {} because "
                             "it's not found in the registry".
                             format(self, disconn.name))
                return
        count, last = self.lastcheck.get(disconn.uid, (0, 0))
        dname = self.getRemoteName(disconn)
        # TODO come back to ratcheting retries
        # secsSinceLastCheck = cur - last
        # secsToWait = self.ratchet.get(count)
        # secsToWaitNext = self.ratchet.get(count + 1)
        # if secsSinceLastCheck > secsToWait:
        # extra = "" if not last else "; needed to wait at least {} and " \
        #                             "waited {} (next try will be {} " \
        #                             "seconds)".format(round(secsToWait, 2),
        #                                         round(secsSinceLastCheck, 2),
        #                                         round(secsToWaitNext, 2)))

        logger.debug("{} retrying to connect with {}".
                     format(self, dname))
        self.lastcheck[disconn.uid] = count + 1, cur
        # self.nextCheck = min(self.nextCheck,
        #                      cur + secsToWaitNext)
        if disconn.joinInProcess():
            logger.debug("waiting, because join is already in "
                         "progress")
        elif disconn.joined:
            self.updateStamp()
            self.allow(uid=disconn.uid, cascade=True, timeout=20)
            logger.debug("{} disconnected node {} is joined".format(
                self, disconn.name), extra={"cli": "STATUS"})
        else:
            self.connect(dname, disconn.uid)

    def findInNodeRegByHA(self, remoteHa):
        """
        Returns the name of the remote by HA if found in the node registry, else
        returns None
        """
        regName = [nm for nm, ha in self.registry.items()
                   if self.sameAddr(ha, remoteHa)]
        if len(regName) > 1:
            raise RuntimeError("more than one node registry entry with the "
                               "same ha {}: {}".format(remoteHa, regName))
        if regName:
            return regName[0]
        return None

    def reconcileNodeReg(self):
        """
        Handle remotes missing from the node registry and clean up old remotes
        no longer in this node's registry.

        1. nice bootstrap
        2. force bootstrap
        3. retry connections

        1. not in remotes
        2.     in remotes, not joined, not allowed, not join in process
        3.     in remotes, not joined, not allowed,     join in process
        4.     in remotes,     joined, not allowed, not allow in process
        5.     in remotes,     joined, not allowed,     allow in process
        6.     in remotes,     joined,     allowed,

        :return: the missing remotes
        """
        matches = set()  # good matches found in nodestack remotes
        legacy = set()  # old remotes that are no longer in registry
        conflicts = set()  # matches found, but the ha conflicts
        logger.debug("{} nodereg is {}".
                     format(self, self.registry.items()))
        logger.debug("{} remotes are {}".
                     format(self, [r.name for r in self.remotes.values()]))

        for r in self.remotes.values():
            if r.name in self.registry:
                if self.sameAddr(r.ha, self.registry[r.name]):
                    matches.add(r.name)
                    logger.debug("{} matched remote is {} {}".
                                 format(self, r.uid, r.ha))
                else:
                    conflicts.add((r.name, r.ha))
                    # error("{} ha for {} doesn't match. ha of remote is {} but "
                    #       "should be {}".
                    #       format(self, r.name, r.ha, self.registry[r.name]))
                    logger.error("{} ha for {} doesn't match. ha of remote is {} but "
                                 "should be {}".
                                 format(self, r.name, r.ha, self.registry[r.name]))
            else:
                regName = self.findInNodeRegByHA(r.ha)

                # This change fixes test
                # `testNodeConnectionAfterKeysharingRestarted` in
                # `test_node_connection`
                # regName = [nm for nm, ha in self.nodeReg.items() if ha ==
                #            r.ha and (r.joined or r.joinInProcess())]
                logger.debug("{} unmatched remote is {} {}".
                             format(self, r.uid, r.ha))
                if regName:
                    logger.debug("{} forgiving name mismatch for {} with same "
                                 "ha {} using another name {}".
                                 format(self, regName, r.ha, r.name))
                    matches.add(regName)
                else:
                    logger.debug("{} found a legacy remote {} "
                                 "without a matching ha {}".
                                 format(self, r.name, r.ha))
                    logger.info(str(self.registry))
                    legacy.add(r)

        # missing from remotes... need to connect
        missing = set(self.registry.keys()) - matches

        if len(missing) + len(matches) + len(conflicts) != len(self.registry):
            logger.error("Error reconciling nodeReg with remotes")
            logger.error("missing: {}".format(missing))
            logger.error("matches: {}".format(matches))
            logger.error("conflicts: {}".format(conflicts))
            logger.error("nodeReg: {}".format(self.registry.keys()))
            logger.error("Error reconciling nodeReg with remotes; see logs")

        if conflicts:
            logger.error("found conflicting address information {} in registry"
                         .format(conflicts))
        if legacy:
            for l in legacy:
                logger.error("{} found legacy entry [{}, {}] in remotes, "
                             "that were not in registry".
                             format(self, l.name, l.ha))
                self.removeRemote(l)
        return missing

    def remotesByConnected(self):
        """
        Partitions the remotes into connected and disconnected

        :return: tuple(connected remotes, disconnected remotes)
        """
        conns, disconns = [], []
        for r in self.remotes.values():
            array = conns if Stack.isRemoteConnected(r) else disconns
            array.append(r)
        return conns, disconns

    def getRemoteName(self, remote):
        """
        Returns the name of the remote object if found in node registry.

        :param remote: the remote object
        """
        if remote.name not in self.registry:
            find = [name for name, ha in self.registry.items()
                    if ha == remote.ha]
            assert len(find) == 1
            return find[0]
        return remote.name


class Batched(MessageProcessor):
    """
    A mixin to allow batching of requests to be send to remotes.
    """

    def __init__(self):
        """
        :param self: 'NodeStacked'
        """
        self.outBoxes = {}  # type: Dict[int, deque]

    def _enqueue(self, msg: Any, rid: int, signer: Signer) -> None:
        """
        Enqueue the message into the remote's queue.

        :param msg: the message to enqueue
        :param rid: the id of the remote node
        """
        payload = self.prepForSending(msg, signer)
        if rid not in self.outBoxes:
            self.outBoxes[rid] = deque()
        self.outBoxes[rid].append(payload)

    def _enqueueIntoAllRemotes(self, msg: Any, signer: Signer) -> None:
        """
        Enqueue the specified message into all the remotes in the nodestack.

        :param msg: the message to enqueue
        """
        for rid in self.remotes.keys():
            self._enqueue(msg, rid, signer)

    def send(self, msg: Any, *rids: Iterable[int], signer: Signer=None) -> None:
        """
        Enqueue the given message into the outBoxes of the specified remotes
         or into the outBoxes of all the remotes if rids is None

        :param msg: the message to enqueue
        :param rids: ids of the remotes to whose outBoxes
         this message must be enqueued
        """
        if rids:
            for r in rids:
                self._enqueue(msg, r, signer)
        else:
            self._enqueueIntoAllRemotes(msg, signer)

    def flushOutBoxes(self) -> None:
        """
        Clear the outBoxes and transmit batched messages to remotes.
        """
        removedRemotes = []
        for rid, msgs in self.outBoxes.items():
            try:
                dest = self.remotes[rid].name
            except KeyError:
                removedRemotes.append(rid)
                continue
            if msgs:
                if len(msgs) == 1:
                    msg = msgs.popleft()
                    # Setting timeout to never expire
                    self.transmit(msg, rid, timeout=self.messageTimeout)
                    logger.trace("{} sending msg {} to {}".format(self, msg, dest))
                else:
                    logger.debug("{} batching {} msgs to {} into one transmission".
                                 format(self, len(msgs), dest))
                    logger.trace("    messages: {}".format(msgs))
                    batch = Batch([], None)
                    while msgs:
                        batch.messages.append(msgs.popleft())
                    # don't need to sign the batch, when the composed msgs are
                    # signed
                    payload = self.prepForSending(batch)
                    logger.trace("{} sending payload to {}: {}".format(self,
                                                                       dest,
                                                                       payload))
                    # Setting timeout to never expire
                    self.transmit(payload, rid, timeout=self.messageTimeout)
        for rid in removedRemotes:
            logger.warning("{} rid {} has been removed".format(self, rid),
                           extra={"cli": False})
            msgs = self.outBoxes[rid]
            if msgs:
                self.discard(msgs, "rid {} no longer available".format(rid),
                             logMethod=logger.debug)
            del self.outBoxes[rid]


class ClientStack(SimpleStack):
    def __init__(self, stackParams: dict, msgHandler: Callable):
        # The client stack needs to be mutable unless we explicitly decide
        # not to
        stackParams["mutable"] = stackParams.get("mutable", True)
        SimpleStack.__init__(self, stackParams, msgHandler)
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
            logger.error("{} unable to send message {} to client {}; Exception: {}"
                         .format(self, msg, remoteName, ex.__repr__()))

    def transmitToClients(self, msg: Any, remoteNames: List[str]):
        for nm in remoteNames:
            self.transmitToClient(msg, nm)


class NodeStack(Batched, KITStack):
    def __init__(self, stackParams: dict, msgHandler: Callable,
                 registry: Dict[str, HA], sighex: str=None):
        Batched.__init__(self)
        # TODO: Just to get around the restriction of port numbers changed on
        # Azure. Remove this soon to relax port numbers only but not IP.
        stackParams["mutable"] = stackParams.get("mutable", True)
        KITStack.__init__(self, stackParams, msgHandler, registry, sighex)

    def start(self):
        KITStack.start(self)
        logger.info("{} listening for other nodes at {}:{}".
                    format(self, *self.ha),
                    extra={"tags": ["node-listening"]})

