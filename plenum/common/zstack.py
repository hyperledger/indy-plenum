import inspect
import json
import shutil
from binascii import hexlify
from collections import deque
from typing import Dict, Mapping, Callable, Any, List, Tuple
import os
import sys
from typing import Set

import time

import asyncio
import zmq
import zmq.asyncio
import zmq.auth
# TODO: Dont use `raet.nacling` as raet needs to be pulled out.
from raet.nacling import Signer, Verifier
from zmq.utils import z85
from zmq.utils.monitor import recv_monitor_message

from plenum.common.authenticator import AsyncioAuthenticator, \
    MyThreadAuthenticator, MyAuthenticator
from plenum.common.batched import Batched
from plenum.common.log import getlogger
from plenum.common.network_interface import NetworkInterface
from plenum.common.ratchet import Ratchet
from plenum.common.txn import BATCH
from plenum.common.types import HA, OP_FIELD_NAME, f
from plenum.common.util import randomString
from plenum.common.z_util import createEncAndSigKeys, \
    moveKeyFilesToCorrectLocations

logger = getlogger()

LINGER_TIME = 20


# TODO: Separate directories are maintainer for public keys and verification
# keys of remote, same direcotry can be used, infact preserve only
# verification key and generate public key from that. Same concern regarding
# signing and private keys


class Remote:
    def __init__(self, name, ha, verKey, publicKey,
                 *args, **kwargs):
        # TODO, remove *args, **kwargs after removing raet

        # Every remote has a unique name per stack, the name can be the
        # public key of the other end
        self.name = name
        self.ha = ha
        # self.publicKey is the public key of the other end of the remote
        self.publicKey = publicKey
        # self.verKey is the verification key of the other end of the remote
        self.verKey = verKey
        self.socket = None
        # TODO: A stack should have a monitor and it should identify remote
        # by endpoint
        # Helps to see if socket got disconnected
        self.monitorSock = None

        self.isConnected = False
        # Currently keeping uid field to resemble RAET RemoteEstate
        self.uid = name

    def __repr__(self):
        return '{}:{}'.format(self.name, self.ha)

    def connect(self, context, localPubKey, localSecKey, typ=None):
        typ = typ or zmq.DEALER
        sock = context.socket(typ)
        sock.curve_publickey = localPubKey
        sock.curve_secretkey = localSecKey
        sock.curve_serverkey = self.publicKey
        sock.identity = localPubKey
        sock.setsockopt(zmq.PROBE_ROUTER, 1)

        # monitorLoc = 'inproc://monitor.{}{}'.format(self.uid, randomString(10))
        # monitorSock = context.socket(zmq.PAIR)
        # monitorSock.connect(monitorLoc)
        # monitorSock.linger = 0
        # sock.monitor(monitorLoc, zmq.EVENT_ALL)

        addr = 'tcp://{}:{}'.format(*self.ha)
        sock.connect(addr)
        self.socket = sock

    def disconnect(self):
        logger.debug('disconnecting remote {}'.format(self))
        if self.socket:
            # Do not close self.monitorSock before disabling monitor as
            # this will freeze the code
            # logger.debug('{} has monitor, {}:{}:{}'.
            #              format(self, self.socket._monitor_socket.FD,
            #                     self.socket._monitor_socket.LAST_ENDPOINT,
            #                     self.socket._monitor_socket.closed))
            # disable_monitor has its operations in reverse order so doing
            # them manually in the correct order
            # self.socket.disable_monitor()
            self.socket.monitor(None, 0)
            self.socket._monitor_socket = None
            # logger.debug('{} 111'.format(self))
            self.socket.close()
            # logger.debug('{} 222'.format(self))
            self.socket = None
        else:
            logger.debug('{} close was closed on a null socket, maybe close is '
                         'being called twice.'.format(self))

        # if self.monitorSock:
        #     self.monitorSock.close()
        #     self.monitorSock = None

        self.isConnected = False


# TODO: Use Async io

class ZStack(NetworkInterface):
    # Assuming only one listener per stack for now.

    PublicKeyDirName = 'public_keys'
    PrivateKeyDirName = 'private_keys'
    VerifKeyDirName = 'verif_keys'
    SigKeyDirName = 'sig_keys'

    sigLen = 64
    pingMessage = 'pi'
    pongMessage = 'po'

    # TODO: This is not implemented, implement this
    messageTimeout = 3

    def __init__(self, name, ha, basedirpath, msgHandler, restricted=True,
                 seed=None, onlyListener=False, *args, **kwargs):
        # TODO, remove *args, **kwargs after removing raet
        self.name = name
        self.ha = ha
        self.basedirpath = basedirpath
        self.msgHandler = msgHandler
        self.seed = seed

        self.homeDir = None
        # As of now there would be only one file in secretKeysDir and sigKeyDir
        self.publicKeysDir = None
        self.secretKeysDir = None
        self.verifKeyDir = None
        self.sigKeyDir = None

        self.signer = None
        self.verifiers = {}

        self.setupDirs()
        self.setupOwnKeysIfNeeded()
        self.setupSigning()

        # self.poller = zmq.asyncio.Poller()

        self.restricted = restricted

        self.ctx = None  # type: Context
        self.listener = None
        self.auth = None

        # Each remote is identified uniquely by the name
        self.remotes = {}  # type: Dict[str, Remote]

        self.remotesByKeys = {}

        # Indicates if this stack will maintain any remotes or will
        # communicate simply to listeners. Used in ClientZStack
        self.onlyListener = onlyListener
        self.peersWithoutRemotes = set()

        self._conns = set()  # type: Set[str]

        self.rxMsgs = deque()
        self.created = time.perf_counter()

    @staticmethod
    def keyDirNames():
        return ZStack.PublicKeyDirName, ZStack.PrivateKeyDirName, \
               ZStack.VerifKeyDirName, ZStack.SigKeyDirName

    def __repr__(self):
        return self.name

    @staticmethod
    def homeDirPath(baseDirPath, name):
        return os.path.join(baseDirPath, name)

    @staticmethod
    def publicDirPath(homeDirPath):
        return os.path.join(homeDirPath, ZStack.PublicKeyDirName)

    @staticmethod
    def secretDirPath(homeDirPath):
        return os.path.join(homeDirPath, ZStack.PrivateKeyDirName)

    @staticmethod
    def verifDirPath(homeDirPath):
        return os.path.join(homeDirPath, ZStack.VerifKeyDirName)

    @staticmethod
    def sigDirPath(homeDirPath):
        return os.path.join(homeDirPath, ZStack.SigKeyDirName)

    def setupDirs(self):
        self.homeDir = self.homeDirPath(self.basedirpath, self.name)
        self.publicKeysDir = self.publicDirPath(self.homeDir)
        self.secretKeysDir = self.secretDirPath(self.homeDir)
        self.verifKeyDir = self.verifDirPath(self.homeDir)
        self.sigKeyDir = self.sigDirPath(self.homeDir)

        for d in (self.homeDir, self.publicKeysDir, self.secretKeysDir,
                  self.verifKeyDir, self.sigKeyDir):
            os.makedirs(d, exist_ok=True)

    def setupOwnKeysIfNeeded(self):
        if not os.listdir(self.sigKeyDir):
            # If signing keys are not present, secret (private keys) should
            # not be present since they should be converted keys.
            assert not os.listdir(self.secretKeysDir)
            # Seed should be present
            assert self.seed, 'Keys are not setup for {}'.format(self)
            logger.info("Signing and Encryption keys were not found. "
                        "Creating them now", extra={"cli": False})
            tdirS = os.path.join(self.homeDir, '__skeys__')
            tdirE = os.path.join(self.homeDir, '__ekeys__')
            os.makedirs(tdirS, exist_ok=True)
            os.makedirs(tdirE, exist_ok=True)
            createEncAndSigKeys(tdirE, tdirS, self.name, self.seed)
            moveKeyFilesToCorrectLocations(tdirE, self.publicKeysDir,
                                           self.secretKeysDir)
            moveKeyFilesToCorrectLocations(tdirS, self.verifKeyDir,
                                           self.sigKeyDir)
            shutil.rmtree(tdirE)
            shutil.rmtree(tdirS)

    def setupAuth(self, restricted=True, force=False):
        if self.auth and not force:
            raise RuntimeError('Listener already setup')
        location = self.publicKeysDir if restricted else zmq.auth.CURVE_ALLOW_ANY
        # self.auth = AsyncioAuthenticator(self.ctx)
        self.auth = MyAuthenticator(self.ctx)
        self.auth.start()
        self.auth.allow('0.0.0.0')
        self.auth.configure_curve(domain='*', location=location)

    def setupSigning(self):
        # Setup its signer from the signing key stored at disk and for all
        # verification keys stored at disk, add Verifier
        _, sk = self.selfSigKeys
        self.signer = Signer(z85.decode(sk))
        for vk in self.getAllVerKeys():
            self.addVerifier(vk)

    def addVerifier(self, verkey):
        self.verifiers[verkey] = Verifier(z85.decode(verkey))

    def start(self, restricted=None, reSetupAuth=True):
        # self.ctx = zmq.asyncio.Context.instance()
        self.ctx = zmq.Context.instance()
        restricted = self.restricted if restricted is None else restricted
        logger.info('{} starting with restricted as {} and reSetupAuth '
                    'as {}'.format(self, restricted, reSetupAuth),
                    extra={"cli": False, "demo": False})
        self.setupAuth(restricted, force=reSetupAuth)
        self.open()

    def stop(self):
        if self.opened:
            logger.info('stack {} closing its listener'.format(self),
                        extra={"cli": False, "demo": False})
            self.close()
        # TODO: Uncommenting this stops hangs the code, setting LINGER_TIME
        # does not help. Find a solution
        # self.ctx.term()
        logger.info("stack {} stopped".format(self),
                    extra={"cli": False, "demo": False})

    @property
    def opened(self):
        return self.listener is not None

    def open(self):
        # noinspection PyUnresolvedReferences
        self.listener = self.ctx.socket(zmq.ROUTER)
        # noinspection PyUnresolvedReferences
        # self.poller.register(self.listener, zmq.POLLIN)
        public, secret = self.selfEncKeys
        self.listener.curve_secretkey = secret
        self.listener.curve_publickey = public
        self.listener.curve_server = True
        self.listener.identity = self.publicKey
        logger.debug('{} will bind its listener at {}'.format(self, self.ha[1]))
        self.listener.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.listener.setsockopt(zmq.PROBE_ROUTER, 1)
        self.listener.bind(
            'tcp://*:{}'.format(self.ha[1]))

    def close(self):
        self.listener.unbind(self.listener.LAST_ENDPOINT)
        self.listener.close()
        self.listener = None
        logger.debug('{} starting to disconnect remotes'.format(self))
        for r in self.remotes.values():
            r.disconnect()
            self.remotesByKeys.pop(r.publicKey, None)

        self.remotes = {}
        if self.remotesByKeys:
            logger.warn('{} found remotes that were only in remotesByKeys and '
                        'not in remotes. This is suspicious')
            for r in self.remotesByKeys.values():
                r.disconnect()
            self.remotesByKeys = {}
        self._conns = set()

    @property
    def selfEncKeys(self):
        serverSecretFile = os.path.join(self.secretKeysDir,
                                        "{}.key_secret".format(self.name))
        return zmq.auth.load_certificate(serverSecretFile)

    @property
    def selfSigKeys(self):
        serverSecretFile = os.path.join(self.sigKeyDir,
                                        "{}.key_secret".format(self.name))
        return zmq.auth.load_certificate(serverSecretFile)

    @property
    def isRestricted(self):
        return not self.auth.allow_any if self.auth is not None \
            else self.restricted

    @property
    def isKeySharing(self):
        # TODO: Change name after removing raet
        return not self.isRestricted

    @staticmethod
    def isRemoteConnected(r: Remote) -> bool:
        """
        A node is considered to be connected if it is joined, allowed and alived.

        :param r: the remote to check
        """
        return r.isConnected

    def isConnectedTo(self, name: str = None, ha: Tuple = None):
        if self.onlyListener:
            return self.hasRemote(name)
        return super().isConnectedTo(name, ha)

    def hasRemote(self, name):
        if self.onlyListener:
            if isinstance(name, str):
                name = name.encode()
            if name in self.peersWithoutRemotes:
                return True
        return super().hasRemote(name)

    def removeRemoteByName(self, name: str):
        if self.onlyListener:
            if name in self.peersWithoutRemotes:
                self.peersWithoutRemotes.remove(name)
                return True
        else:
            return super().removeRemoteByName(name)

    def getHa(self, name):
        # Return HA as None when its a `peersWithoutRemote`
        if self.onlyListener:
            if isinstance(name, str):
                name = name.encode()
            if name in self.peersWithoutRemotes:
                return None
        return super().getHa(name)

    async def service(self, limit=None) -> int:
        """
        Service `limit` number of received messages in this stack.

        :param limit: the maximum number of messages to be processed. If None,
        processes all of the messages in rxMsgs.
        :return: the number of messages processed.
        """
        if self.listener:
            await self._serviceStack(self.age)
        else:
            logger.debug("{} is stopped".format(self))

        r = len(self.rxMsgs)
        if r > 0:
            pracLimit = limit if limit else sys.maxsize
            return self.processReceived(pracLimit)
        return 0

    async def _serviceStack(self, age):
        # TODO: age is unused

        def verifyAndAppend(msg, ident):
            if self.verify(msg, ident):
                self.rxMsgs.append((msg[:-self.sigLen].decode(), ident))
            else:
                logger.error('{} got error while verifying message {} from {}'
                             .format(self, msg, ident))

        # TODO: If a stack is being bombarded with messages this can lead to the
        # stack always busy in receiving messages and never getting time to
        # complete processing fo messages.
        # TODO: Optimize this
        i = 0
        while True:
            try:
                # ident, msg = await self.listener.recv_multipart(
                #     flags=zmq.NOBLOCK)
                ident, msg = self.listener.recv_multipart(flags=zmq.NOBLOCK)
                if not msg:
                    # Router probing sends empty message on connection
                    continue
                if self.onlyListener and ident not in self.remotesByKeys:
                    self.peersWithoutRemotes.add(ident)
                i += 1
                verifyAndAppend(msg, ident)
            except zmq.Again:
                if i > 0:
                    logger.trace('{} got {} messages through listener'.
                                 format(self, i))
                break

        for ident, remote in self.remotesByKeys.items():
            if not remote.socket:
                continue

            if self.shouldReconnect(remote):
                self.reconnectRemote(remote)
                # This connect wont be done immediately so service this socket
                # sometime later
                continue

            sock = remote.socket

            i = 0
            while True:
                try:
                    # ident, msg = await sock.recv(flags=zmq.NOBLOCK)
                    # msg = sock.recv(flags=zmq.NOBLOCK)
                    msg, = sock.recv_multipart(flags=zmq.NOBLOCK)
                    if not msg:
                        # Router probing sends empty message on connection
                        continue
                    i += 1
                    verifyAndAppend(msg, ident)
                except zmq.Again:
                    if i > 0:
                        logger.trace('{} got {} messages through remote {}'.
                                     format(self, i, remote))
                    break

        return len(self.rxMsgs)

    def processReceived(self, limit):

        def handlePingPong(msg, frm, ident):
            if msg in (self.pingMessage, self.pongMessage):
                if msg == self.pingMessage:
                    logger.trace('{} got ping from {}'.format(self, frm))
                    self.send(self.pongMessage, frm)
                    logger.trace('{} sent pong to {}'.format(self, frm))
                if msg == self.pongMessage:
                    if ident in self.remotesByKeys:
                        self.remotesByKeys[ident].isConnected = True
                    logger.trace('{} got pong from {}'.format(self, frm))
                return True
            return False

        if limit > 0:
            for x in range(limit):
                try:
                    msg, ident = self.rxMsgs.popleft()

                    frm = self.remotesByKeys[ident].name \
                        if ident in self.remotesByKeys else ident

                    r = handlePingPong(msg, frm, ident)
                    if r:
                        continue

                    try:
                        msg = json.loads(msg)
                    except Exception as e:
                        logger.error('Error {} while converting message {} '
                                     'to JSON from {}'.format(e, msg, ident))
                        continue

                    # TODO: Refactor, this should be moved to `Batched`
                    if OP_FIELD_NAME in msg and msg[OP_FIELD_NAME] == BATCH:
                        if f.MSGS.nm in msg and isinstance(msg[f.MSGS.nm], list):
                            relevantMsgs = []
                            for m in msg[f.MSGS.nm]:
                                r = handlePingPong(m, frm, ident)
                                if not r:
                                    relevantMsgs.append(m)

                            if not relevantMsgs:
                                continue
                            msg[f.MSGS.nm] = relevantMsgs

                    self.msgHandler((msg, frm))
                except IndexError:
                    break
            return x
        return 0

    def connect(self, name, ha=None, verKey=None, publicKey=None):
        """
        Connect to the node specified by name.
        """
        if name not in self.remotes:
            if not publicKey:
                try:
                    publicKey = self.getPublicKey(name)
                except KeyError:
                    logger.error("{} could not get {}'s public key from disk"
                                 .format(self, name))
            if not verKey:
                try:
                    verKey = self.getVerKey(name)
                except KeyError:
                    if self.isRestricted:
                        logger.error("Could not get {}'s verification key "
                                     "from disk".format(name))

            if not (ha and publicKey and (not self.isRestricted or verKey)):
                raise ValueError('{} doesnt have enough info to connect. '
                                 'Need ha, public key and verkey. {} {} {}'.
                                 format(name, ha, verKey, publicKey))
            remote = self.addRemote(name, ha, verKey, publicKey)
        else:
            remote = self.remotes[name]

        public, secret = self.selfEncKeys
        remote.connect(self.ctx, public, secret)

        logger.info("{} looking for {} at {}:{}".
                    format(self, name or remote.name, *remote.ha),
                    extra={"cli": "PLAIN", "tags": ["node-looking"]})

        # This should be scheduled as an async task
        self.sendPing(remote)
        return remote.uid

    def shouldReconnect(self, remote):
        # logger.trace('{} getting monitor socket for remote {} with socket state {}'.format(self, remote, remote.socket.closed))
        monitor = remote.socket.get_monitor_socket()
        events = []
        while True:
            try:
                # noinspection PyUnresolvedReferences
                m = recv_monitor_message(monitor, flags=zmq.NOBLOCK)
                events.append(m['event'])
            except zmq.Again:
                break

        if events:
            logger.trace('{} has monitor events for remote {}: {}'.
                         format(self, remote, events))
        # noinspection PyUnresolvedReferences
        if zmq.EVENT_DISCONNECTED in events:
            logger.debug('{} found disconnected event on monitor of remote'
                         ' {}'.format(self, remote))
            events.reverse()
            # noinspection PyUnresolvedReferences
            d = events.index(zmq.EVENT_DISCONNECTED)
            # noinspection PyUnresolvedReferences
            c = events.index(
                zmq.EVENT_CONNECTED) if zmq.EVENT_CONNECTED in events else sys.maxsize
            # noinspection PyUnresolvedReferences
            p = events.index(
                zmq.EVENT_CONNECT_DELAYED) if zmq.EVENT_CONNECT_DELAYED in events else sys.maxsize
            if d < c and d < p:
                # If no connected event
                return True
        return False

    def reconnectRemote(self, remote):
        logger.debug('{} reconnecting to {}'.format(self, remote))
        public, secret = self.selfEncKeys
        remote.disconnect()
        remote.connect(self.ctx, public, secret)
        # from threading import Timer
        # r = Timer(0.5, self.sendPing, (remote, ))
        # r.start()
        self.sendPing(remote)

    def addRemote(self, name, ha, remoteVerkey, remotePublicKey):
        remote = Remote(name, ha, remoteVerkey, remotePublicKey)
        self.remotes[name] = remote
        # TODO: Use weakref to remote below instead
        self.remotesByKeys[remotePublicKey] = remote
        if remoteVerkey:
            self.addVerifier(remoteVerkey)
        else:
            logger.debug('{} adding a remote {}({}) without a verkey'.
                         format(self, name, ha))
        return remote

    def removeRemote(self, remote: Remote, clear=True):
        """
        Currently not using clear
        """
        name = remote.name
        pkey = remote.publicKey
        vkey = remote.verKey
        if name in self.remotes:
            self.remotes.pop(name)
            self.remotesByKeys.pop(pkey, None)
            self.verifiers.pop(vkey, None)
        else:
            logger.warn('No remote named {} present')

    def sendPing(self, remote):
        r = self.send(self.pingMessage, remote.name)
        if r:
            logger.debug('{} pinged {} at {}'.format(self.name, remote.name,
                                                     self.ha))
        else:
            # TODO: This fails the first time as socket is not established,
            # need to make it retriable
            logger.warn('{} failed to ping {} at {}'.
                        format(self.name, remote.name, remote.ha),
                        extra={"cli": False})

    def send(self, msg, remote: str = None):
        if self.onlyListener:
            return self.transmitThroughListener(msg, remote)
        else:
            if remote is None:
                r = []
                for uid in self.remotes:
                    r.append(self.transmit(msg, uid))
                return all(r)
            else:
                return self.transmit(msg, remote)

    def transmit(self, msg, uid, timeout=None):
        # Timeout is unused as of now
        assert uid in self.remotes
        socket = self.remotes[uid].socket
        if socket:
            msg = self.prepMsg(msg)
            try:
                # noinspection PyUnresolvedReferences
                socket.send(self.signedMsg(msg), flags=zmq.NOBLOCK)
                logger.debug(
                    '{} transmitted message {} to {}'.format(self, msg, uid))
                return True
            except zmq.Again as ex:
                logger.debug('{} could not transmit message to {}'.format(self, uid))
                return False
        else:
            logger.warn('{} has uninitialised socket for remote {}'.
                        format(self, self.remotes[uid]))
            return False

    def transmitThroughListener(self, msg, ident):
        if isinstance(ident, str):
            ident = ident.encode()
        if ident not in self.peersWithoutRemotes:
            logger.info('{} not sending message {} to {}'.
                        format(self, msg, ident))
            logger.info("This is a temporary workaround for not being able to "
                        "disconnect a ROUTER's remote")
            return
        msg = self.prepMsg(msg)
        try:
            # noinspection PyUnresolvedReferences
            self.listener.send_multipart([ident, self.signedMsg(msg)],
                                         flags=zmq.NOBLOCK)
            return True
        except zmq.Again:
            return False
        except Exception as e:
            logger.error('{} got error {} while sending through listener to {}'.
                         format(self, e, ident))

    @staticmethod
    def prepMsg(msg):
        if isinstance(msg, Mapping):
            msg = json.dumps(msg)
        if isinstance(msg, str):
            msg = msg.encode()
        assert isinstance(msg, bytes)
        return msg

    def signedMsg(self, msg: bytes, signer: Signer=None):
        # Signing even if keysharing is ON since the other part
        sig = self.signer.signature(msg)
        return msg + sig

    def verify(self, msg, by):
        if self.isKeySharing:
            return True
        if by not in self.remotesByKeys:
            return False
        verKey = self.remotesByKeys[by].verKey
        r = self.verifiers[verKey].verify(msg[-self.sigLen:], msg[:-self.sigLen])
        return r

    @staticmethod
    def loadPubKeyFromDisk(directory, name):
        filePath = os.path.join(directory,
                                "{}.key".format(name))
        try:
            public, _ = zmq.auth.load_certificate(filePath)
            return public
        except (ValueError, IOError) as ex:
            raise KeyError from ex

    @staticmethod
    def loadSecKeyFromDisk(directory, name):
        filePath = os.path.join(directory,
                                "{}.key_secret".format(name))
        try:
            _, secret = zmq.auth.load_certificate(filePath)
            return secret
        except (ValueError, IOError) as ex:
            raise KeyError from ex

    @property
    def publicKey(self):
        return self.getPublicKey(self.name)

    def getPublicKey(self, name):
        return self.loadPubKeyFromDisk(self.publicKeysDir, name)

    @property
    def verKey(self):
        return self.getVerKey(self.name)

    def getVerKey(self, name):
        return self.loadPubKeyFromDisk(self.verifKeyDir, name)

    @property
    def verhex(self):
        return hexlify(z85.decode(self.verKey))

    @property
    def sigKey(self):
        return self.loadSecKeyFromDisk(self.sigKeyDir, self.name)

    # TODO: Change name to sighex after removing raet
    @property
    def keyhex(self):
        return hexlify(z85.decode(self.sigKey))

    @property
    def pubhex(self):
        return hexlify(z85.decode(self.publicKey))

    @property
    def priKey(self):
        return self.loadSecKeyFromDisk(self.secretKeysDir, self.name)

    @property
    def prihex(self):
        return hexlify(z85.decode(self.priKey))

    def getAllVerKeys(self):
        keys = []
        for key_file in os.listdir(self.verifKeyDir):
            if key_file.endswith(".key"):
                serverVerifFile = os.path.join(self.verifKeyDir,
                                               key_file)
                serverPublic, _ = zmq.auth.load_certificate(serverVerifFile)
                keys.append(serverPublic)
        return keys

    def setRestricted(self, restricted: bool):
        if self.isRestricted != restricted:
            logger.info('{} setting restricted to {}'.format(self, restricted))
            self.stop()

            # TODO: REMOVE, it will make code slow, only doing to allow the
            # socket to become available again
            time.sleep(1)

            self.start(restricted, reSetupAuth=True)

    def _safeRemove(self, filePath):
        try:
            os.remove(filePath)
        except Exception as ex:
            logger.info('{} could delete file {} due to {}'.
                        format(self, filePath, ex))

    def clearLocalRoleKeep(self):
        for d in (self.secretKeysDir, self.sigKeyDir):
            filePath = os.path.join(d, "{}.key_secret".format(self.name))
            self._safeRemove(filePath)

        for d in (self.publicKeysDir, self.verifKeyDir):
            filePath = os.path.join(d, "{}.key".format(self.name))
            self._safeRemove(filePath)

    def clearRemoteRoleKeeps(self):
        for d in (self.secretKeysDir, self.sigKeyDir):
            for key_file in os.listdir(d):
                if key_file != '{}.key_secret'.format(self.name):
                    self._safeRemove(os.path.join(d, key_file))

        for d in (self.publicKeysDir, self.verifKeyDir):
            for key_file in os.listdir(d):
                if key_file != '{}.key'.format(self.name):
                    self._safeRemove(os.path.join(d, key_file))

    def clearAllDir(self):
        shutil.rmtree(self.homeDir)

    # TODO: Members below are just for the time till RAET replacement is
    # complete, they need to be removed then.
    @property
    def nameRemotes(self):
        logger.info('{} proxy method used on {}'.
                    format(inspect.stack()[0][3], self))
        return self.remotes

    @property
    def keep(self):
        logger.info('{} proxy method used on {}'.
                    format(inspect.stack()[0][3], self))
        if not hasattr(self, '_keep'):
            self._keep = DummyKeep(self)
        return self._keep

    def clearLocalKeep(self):
        pass

    def clearRemoteKeeps(self):
        pass

    # def addListener(self, ha):
    #     pass
    #
    # @property
    # def defaultListener(self):
    #     pass


class DummyKeep:
    def __init__(self, stack, *args, **kwargs):
        self.stack = stack
        self._auto = 2 if stack.isKeySharing else 0

    @property
    def auto(self):
        logger.info('{} proxy method used on {}'.
                    format(inspect.stack()[0][3], self))
        return self._auto

    @auto.setter
    def auto(self, mode):
        logger.info('{} proxy method used on {}'.
                    format(inspect.stack()[0][3], self))
        # AutoMode.once whose value is 1 is not used os dont care
        if mode != self._auto:
            if mode == 2:
                self.stack.setRestricted(False)
            if mode == 0:
                self.stack.setRestricted(True)


class SimpleZStack(ZStack):
    def __init__(self, stackParams: Dict, msgHandler: Callable, seed=None,
                 onlyListener=False, sighex: str=None):
        # TODO: sighex is unused as of now, remove once raet is removed or
        # maybe use sighex to generate all keys, DECISION DEFERRED

        self.stackParams = stackParams
        self.msgHandler = msgHandler

        # TODO: Ignoring `main` param as of now which determines
        # if the stack will have a listener socket or not.
        name = stackParams['name']
        ha = stackParams['ha']
        basedirpath = stackParams['basedirpath']

        # TODO: Change after removing raet
        auto = stackParams['auto']
        restricted = True if auto == 0 else False

        super().__init__(name, ha, basedirpath, msgHandler=self.msgHandler,
                         restricted=restricted, seed=seed,
                         onlyListener=onlyListener)


class KITZStack(SimpleZStack):
    # Stack which maintains connections mentioned in its registry
    def __init__(self, stackParams: dict, msgHandler: Callable,
                 registry: Dict[str, HA], seed=None, sighex: str = None):
        super().__init__(stackParams, msgHandler, seed=seed, sighex=sighex)
        self.registry = registry

        self.ratchet = Ratchet(a=8, b=0.198, c=-4, base=8, peak=3600)

        # holds the last time we checked remotes
        self.nextCheck = 0

    def maintainConnections(self, force=False):
        """
        Ensure appropriate connections.

        """
        cur = time.perf_counter()
        if cur > self.nextCheck or force:

            self.nextCheck = cur + (6 if self.isKeySharing else 15)
            missing = self.connectToMissing()
            self.retryDisconnected(exclude=missing)
            logger.debug("{} next check for retries in {:.2f} seconds".
                         format(self, self.nextCheck - cur))
            return True
        return False

    def retryDisconnected(self, exclude=None):
        exclude = exclude or {}
        for r in self.remotes.values():
            if r.name not in exclude and not r.isConnected:
                self.reconnectRemote(r)

    def connectToMissing(self):
        """
        Try to connect to the missing nodes

        """
        missing = self.reconcileNodeReg()
        if missing:
            logger.debug("{} found the following missing connections: {}".
                         format(self, ", ".join(missing)))
            for name in missing:
                try:
                    self.connect(name, ha=self.registry[name])
                except ValueError as ex:
                    logger.error('{} cannot connect to {} due to {}'.
                                 format(self, name, ex))
        return missing

    def reconcileNodeReg(self):
        matches = set()
        for r in self.remotes.values():
            if r.name in self.registry:
                if self.sameAddr(r.ha, self.registry[r.name]):
                    matches.add(r.name)
                    logger.debug("{} matched remote is {} {}".
                                 format(self, r.uid, r.ha))

        return set(self.registry.keys()) - matches - {self.name,}

    async def service(self, limit=None):
        self.checkConns()
        self.maintainConnections()
        return await super().service(limit)

    @property
    def notConnectedNodes(self) -> Set[str]:
        """
        Returns the names of nodes in the registry this node is NOT connected
        to.
        """
        return set(self.registry.keys()) - self.conns

    # The next method is copied from KITStack from stacked.py
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


class ClientZStack(SimpleZStack):
    def __init__(self, stackParams: dict, msgHandler: Callable, seed=None):
        SimpleZStack.__init__(self, stackParams, msgHandler, seed=seed,
                              onlyListener=True)
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
        #TODO: Handle `remoteNames`
        for nm in self.peersWithoutRemotes:
            self.transmitToClient(msg, nm)


class NodeZStack(Batched, KITZStack):
    def __init__(self, stackParams: dict, msgHandler: Callable,
                 registry: Dict[str, HA], seed=None, sighex: str=None):
        Batched.__init__(self)
        KITZStack.__init__(self, stackParams, msgHandler, registry=registry,
                           seed=seed, sighex=sighex)

    # TODO: Reconsider defaulting `reSetupAuth` to True.
    def start(self, restricted=None, reSetupAuth=True):
        KITZStack.start(self, restricted=restricted, reSetupAuth=reSetupAuth)
        logger.info("{} listening for other nodes at {}:{}".
                    format(self, *self.ha),
                    extra={"tags": ["node-listening"]})

    # TODO: Members below are just for the time till RAET replacement is
    # complete, they need to be removed then.
    async def serviceLifecycle(self) -> None:
        pass
