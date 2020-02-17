import sys

import asyncio
import zmq
import zmq.asyncio
from zmq.auth import Authenticator
from zmq.auth.thread import _inherit_docstrings, ThreadAuthenticator, \
    AuthenticationThread


# Copying code from zqm classes since no way to inject these dependencies


class MultiZapAuthenticator(Authenticator):
    """
    `Authenticator` supports only one ZAP socket in a single process, this lets
     you have multiple ZAP sockets
    """
    count = 0

    def __init__(self, context=None, encoding='utf-8', log=None):
        MultiZapAuthenticator.count += 1
        super().__init__(context=context, encoding=encoding, log=log)

    def start(self):
        """Create and bind the ZAP socket"""
        self.zap_socket = self.context.socket(zmq.REP)
        self.zap_socket.linger = 1
        zapLoc = 'inproc://zeromq.zap.{}'.format(MultiZapAuthenticator.count)
        self.zap_socket.bind(zapLoc)
        self.log.debug('Starting ZAP at {}'.format(zapLoc))

    def stop(self):
        """Close the ZAP socket"""
        if self.zap_socket:
            self.log.debug(
                'Stopping ZAP at {}'.format(self.zap_socket.LAST_ENDPOINT))
            super().stop()


@_inherit_docstrings
class ThreadMultiZapAuthenticator(ThreadAuthenticator):
    def start(self):
        """Start the authentication thread"""
        # create a socket to communicate with auth thread.
        self.pipe = self.context.socket(zmq.PAIR)
        self.pipe.linger = 1
        self.pipe.bind(self.pipe_endpoint)
        authenticator = MultiZapAuthenticator(self.context, encoding=self.encoding,
                                              log=self.log)
        self.thread = AuthenticationThread(self.context, self.pipe_endpoint,
                                           encoding=self.encoding, log=self.log,
                                           authenticator=authenticator)
        self.thread.start()
        # Event.wait:Changed in version 2.7: Previously, the method always returned None.
        if sys.version_info < (2, 7):
            self.thread.started.wait(timeout=10)
        else:
            if not self.thread.started.wait(timeout=10):
                raise RuntimeError("Authenticator thread failed to start")


class AsyncioAuthenticator(MultiZapAuthenticator):
    """ZAP authentication for use in the asyncio IO loop"""

    def __init__(self, context=None, loop=None):
        super().__init__(context)
        self.loop = loop or asyncio.get_event_loop()
        self.__poller = None
        self.__task = None

    # TODO: Remove this commented method later
    # @asyncio.coroutine
    # def __handle_zap(self):
    #     while True:
    #         events = yield from self.__poller.poll()
    #         if self.zap_socket in dict(events):
    #             msg = yield from self.zap_socket.recv_multipart()
    #             self.handle_zap_message(msg)

    async def __handle_zap(self):
        while True:
            events = await self.__poller.poll()
            if self.zap_socket in dict(events):
                msg = await self.zap_socket.recv_multipart()
                self.handle_zap_message(msg)

    def start(self):
        """Start ZAP authentication"""
        super().start()
        self.__poller = zmq.asyncio.Poller()
        self.__poller.register(self.zap_socket, zmq.POLLIN)
        self.__task = asyncio.ensure_future(self.__handle_zap())

    def stop(self):
        """Stop ZAP authentication"""
        if self.__task:
            self.__task.cancel()
        if self.__poller:
            self.__poller.unregister(self.zap_socket)
            self.__poller = None
        super().stop()
