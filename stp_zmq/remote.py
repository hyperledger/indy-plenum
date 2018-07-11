from stp_core.common.config.util import getConfig
import time
import zmq

from stp_core.common.constants import ZMQ_NETWORK_PROTOCOL
from stp_core.common.log import getlogger
import sys
from zmq.utils.monitor import recv_monitor_message
from zmq.sugar.socket import Socket

logger = getlogger()


def set_keepalive(socket: Socket, config):
    # This assumes the same TCP_KEEPALIVE configuration for all sockets which
    # is not ideal but matches what we do in code
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, config.KEEPALIVE_INTVL)
    socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, config.KEEPALIVE_IDLE)
    socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, config.KEEPALIVE_CNT)


def set_zmq_internal_queue_size(socket: Socket, queue_size: int):
    # set both ZMQ_RCVHWM and ZMQ_SNDHWM
    socket.set_hwm(queue_size)


class Remote:
    def __init__(self, name, ha, verKey, publicKey, queue_size=0, config=None):
        # TODO, remove *args, **kwargs after removing test

        assert name

        # Every remote has a unique name per stack, the name can be the
        # public key of the other end
        self.name = name
        self.ha = ha
        # self.publicKey is the public key of the other end of the remote
        self.publicKey = publicKey
        # self.verKey is the verification key of the other end of the remote
        self.verKey = verKey
        self.queue_size = queue_size
        self.socket = None
        # TODO: A stack should have a monitor and it should identify remote
        # by endpoint

        self._numOfReconnects = 0
        self._isConnected = False
        self._lastConnectedAt = None
        self.config = config or getConfig()
        self.uid = name

    def __repr__(self):
        return '{}:{}'.format(self.name, self.ha)

    @property
    def isConnected(self):
        if not self._isConnected:
            return False
        lost = self.hasLostConnection
        if lost:
            self._isConnected = False
            return False
        return True

    def setConnected(self):
        self._numOfReconnects += 1
        self._isConnected = True
        self._lastConnectedAt = time.perf_counter()

    def firstConnect(self):
        return self._numOfReconnects == 0

    def connect(self, context, localPubKey, localSecKey, typ=None):
        typ = typ or zmq.DEALER
        sock = context.socket(typ)
        sock.curve_publickey = localPubKey
        sock.curve_secretkey = localSecKey
        sock.curve_serverkey = self.publicKey
        sock.identity = localPubKey
        set_keepalive(sock, self.config)
        set_zmq_internal_queue_size(sock, self.queue_size)
        addr = '{protocol}://{}:{}'.format(*self.ha, protocol=ZMQ_NETWORK_PROTOCOL)
        sock.connect(addr)
        self.socket = sock
        logger.trace('connecting socket {} {} to remote {}'.
                     format(self.socket.FD, self.socket.underlying, self))

    def disconnect(self):
        logger.debug('disconnecting remote {}'.format(self))
        if self.socket:
            logger.trace('disconnecting socket {} {}'.
                         format(self.socket.FD, self.socket.underlying))

            if self.socket._monitor_socket:
                logger.trace('{} closing monitor socket'.format(self))
                self.socket._monitor_socket.linger = 0
                self.socket.monitor(None, 0)
                self.socket._monitor_socket = None
                # self.socket.disable_monitor()
            self.socket.close(linger=0)
            self.socket = None
        else:
            logger.info('{} close was called on a null socket, maybe close is '
                        'being called twice.'.format(self))

        self._isConnected = False

    @property
    def hasLostConnection(self):

        if self.socket is None:
            logger.debug('Remote {} already disconnected'.format(self))
            return False

        events = self._lastSocketEvents()

        if events:
            logger.trace('Remote {} has monitor events: {}'.
                         format(self, events))

        # noinspection PyUnresolvedReferences
        if zmq.EVENT_DISCONNECTED in events or zmq.EVENT_CLOSED in events:
            logger.debug('{} found disconnected event on monitor'.format(self))

            # Reverse events list since list has no builtin to get last index
            events.reverse()

            def eventIndex(eventName):
                try:
                    return events.index(eventName)
                except ValueError:
                    return sys.maxsize

            connected = eventIndex(zmq.EVENT_CONNECTED)
            delayed = eventIndex(zmq.EVENT_CONNECT_DELAYED)
            disconnected = min(eventIndex(zmq.EVENT_DISCONNECTED),
                               eventIndex(zmq.EVENT_CLOSED))
            if disconnected < connected and disconnected < delayed:
                return True

        return False

    def _lastSocketEvents(self, nonBlock=True):
        return self._get_monitor_events(self.socket, nonBlock)

    @staticmethod
    def _get_monitor_events(socket, non_block=True):
        # It looks strange to call get_monitor_socket() each time we
        # want to get it instead of get it once and save reference.
        # May side effects here, will create a ticket to check and clean
        # up the implementation.
        monitor = socket.get_monitor_socket()
        events = []
        # noinspection PyUnresolvedReferences
        flags = zmq.NOBLOCK if non_block else 0
        while True:
            try:
                # noinspection PyUnresolvedReferences
                message = recv_monitor_message(monitor, flags)
                events.append(message['event'])
            except zmq.Again:
                break
        return events
