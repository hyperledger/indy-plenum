import inspect
import json

from plenum.common.constants import OP_FIELD_NAME, BATCH
from plenum.common.metrics_collector import NullMetricsCollector
from plenum.common.startable import Mode
from plenum.common.util import z85_to_friendly
from stp_core.common.config.util import getConfig
from stp_core.common.constants import CONNECTION_PREFIX, ZMQ_NETWORK_PROTOCOL
from stp_zmq.client_message_provider import ClientMessageProvider


import os
import shutil
import sys
import time
from binascii import hexlify, unhexlify
from collections import deque
from typing import Mapping, Tuple, Any, Union, Optional, NamedTuple

from common.exceptions import PlenumTypeError, PlenumValueError

# import stp_zmq.asyncio
import zmq.auth
from stp_core.crypto.nacl_wrappers import Signer, Verifier
from stp_core.crypto.util import isHex, ed25519PkToCurve25519
from stp_core.network.exceptions import PublicKeyNotFoundOnDisk, VerKeyNotFoundOnDisk
from stp_zmq.authenticator import MultiZapAuthenticator
from zmq.utils import z85
from zmq.utils.monitor import recv_monitor_message

import zmq
from stp_core.common.log import getlogger
from stp_core.network.network_interface import NetworkInterface
from stp_zmq.util import createEncAndSigKeys, \
    moveKeyFilesToCorrectLocations, createCertsFromKeys
from stp_zmq.remote import Remote, set_keepalive, set_zmq_internal_queue_size
from plenum.common.exceptions import InvalidMessageExceedingSizeException, BaseExc
from stp_core.validators.message_length_validator import MessageLenValidator

logger = getlogger()

Quota = NamedTuple("Quota", [("count", int), ("size", int)])


# TODO: Use Async io
# TODO: There a number of methods related to keys management,
# they can be moved to some class like KeysManager
class ZStack(NetworkInterface):
    # Assuming only one listener per stack for now.

    PublicKeyDirName = 'public_keys'
    PrivateKeyDirName = 'private_keys'
    VerifKeyDirName = 'verif_keys'
    SigKeyDirName = 'sig_keys'

    sigLen = 64
    pingMessage = 'pi'
    pongMessage = 'po'
    healthMessages = {pingMessage.encode(), pongMessage.encode()}

    # TODO: This is not implemented, implement this
    messageTimeout = 3

    _RemoteClass = Remote

    def __init__(self, name, ha, basedirpath, msgHandler, restricted=True,
                 seed=None, onlyListener=False, config=None, msgRejectHandler=None, queue_size=0,
                 create_listener_monitor=False, metrics=NullMetricsCollector(),
                 mt_incoming_size=None, mt_outgoing_size=None, timer=None):
        self._name = name
        self.ha = ha
        self.basedirpath = basedirpath
        self.msgHandler = msgHandler
        self.seed = seed
        self.queue_size = queue_size
        self.config = config or getConfig()
        self.msgRejectHandler = msgRejectHandler or self.__defaultMsgRejectHandler

        self._node_mode = None
        self._stashed_unknown_remote_msgs = deque(maxlen=self.config.ZMQ_STASH_UNKNOWN_REMOTE_MSGS_QUEUE_SIZE)

        self.metrics = metrics
        self.mt_incoming_size = mt_incoming_size
        self.mt_outgoing_size = mt_outgoing_size

        self.listenerQuota = self.config.DEFAULT_LISTENER_QUOTA
        self.listenerSize = self.config.DEFAULT_LISTENER_SIZE
        self.senderQuota = self.config.DEFAULT_SENDER_QUOTA
        self.msgLenVal = MessageLenValidator(self.config.MSG_LEN_LIMIT)

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

        # self.poller = test.asyncio.Poller()

        self.restricted = restricted

        self.ctx = None  # type: Context
        self.listener = None
        self.create_listener_monitor = create_listener_monitor
        self.listener_monitor = None
        self.auth = None

        # Each remote is identified uniquely by the name
        self._remotes = {}  # type: Dict[str, Remote]

        self.remotesByKeys = {}

        self.remote_ping_stats = {}

        # Indicates if this stack will maintain any remotes or will
        # communicate simply to listeners. Used in ClientZStack
        self.onlyListener = onlyListener

        self._conns = set()  # type: Set[str]

        self.rxMsgs = deque()
        self._created = time.perf_counter()

        self.last_heartbeat_at = None

        self._stashed_pongs = set()
        self._received_pings = set()
        self._client_message_provider = ClientMessageProvider(self.name,
                                                              self.config,
                                                              self.prepare_to_send,
                                                              self.metrics,
                                                              self.mt_outgoing_size,
                                                              timer)

    def __defaultMsgRejectHandler(self, reason: str, frm):
        pass

    @property
    def remotes(self):
        return self._remotes

    @property
    def created(self):
        return self._created

    @property
    def name(self):
        return self._name

    def set_mode(self, value):
        self._node_mode = value

    @staticmethod
    def isRemoteConnected(r) -> bool:
        return r.isConnected

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
            logger.info('No remote named {} present')

    @staticmethod
    def initLocalKeys(name, baseDir, sigseed, override=False):
        sDir = os.path.join(baseDir, '__sDir')
        eDir = os.path.join(baseDir, '__eDir')
        os.makedirs(sDir, exist_ok=True)
        os.makedirs(eDir, exist_ok=True)
        (public_key, secret_key), (verif_key, sig_key) = \
            createEncAndSigKeys(eDir, sDir, name, seed=sigseed)

        homeDir = ZStack.homeDirPath(baseDir, name)
        verifDirPath = ZStack.verifDirPath(homeDir)
        sigDirPath = ZStack.sigDirPath(homeDir)
        secretDirPath = ZStack.secretDirPath(homeDir)
        pubDirPath = ZStack.publicDirPath(homeDir)
        for d in (homeDir, verifDirPath, sigDirPath, secretDirPath, pubDirPath):
            os.makedirs(d, exist_ok=True)

        moveKeyFilesToCorrectLocations(sDir, verifDirPath, sigDirPath)
        moveKeyFilesToCorrectLocations(eDir, pubDirPath, secretDirPath)

        shutil.rmtree(sDir)
        shutil.rmtree(eDir)
        return hexlify(public_key).decode(), hexlify(verif_key).decode()

    @staticmethod
    def initRemoteKeys(name, remoteName, baseDir, verkey, override=False):
        homeDir = ZStack.homeDirPath(baseDir, name)
        verifDirPath = ZStack.verifDirPath(homeDir)
        pubDirPath = ZStack.publicDirPath(homeDir)
        for d in (homeDir, verifDirPath, pubDirPath):
            os.makedirs(d, exist_ok=True)

        if isHex(verkey):
            verkey = unhexlify(verkey)

        createCertsFromKeys(verifDirPath, remoteName, z85.encode(verkey))
        public_key = ed25519PkToCurve25519(verkey)
        createCertsFromKeys(pubDirPath, remoteName, z85.encode(public_key))

    def onHostAddressChanged(self):
        # we don't store remote data like ip, port, domain name, etc, so
        # nothing to do here
        pass

    @staticmethod
    def areKeysSetup(name, baseDir):
        homeDir = ZStack.homeDirPath(baseDir, name)
        verifDirPath = ZStack.verifDirPath(homeDir)
        pubDirPath = ZStack.publicDirPath(homeDir)
        sigDirPath = ZStack.sigDirPath(homeDir)
        secretDirPath = ZStack.secretDirPath(homeDir)
        for d in (verifDirPath, pubDirPath):
            if not os.path.isfile(os.path.join(d, '{}.key'.format(name))):
                return False
        for d in (sigDirPath, secretDirPath):
            if not os.path.isfile(os.path.join(d, '{}.key_secret'.format(name))):
                return False
        return True

    @staticmethod
    def keyDirNames():
        return ZStack.PublicKeyDirName, ZStack.PrivateKeyDirName, \
            ZStack.VerifKeyDirName, ZStack.SigKeyDirName

    @staticmethod
    def getHaFromLocal(name, basedirpath):
        return None

    def __repr__(self):
        return self.name

    @staticmethod
    def homeDirPath(baseDirPath, name):
        return os.path.join(os.path.expanduser(baseDirPath), name)

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

    @staticmethod
    def learnKeysFromOthers(baseDir, name, others):
        homeDir = ZStack.homeDirPath(baseDir, name)
        verifDirPath = ZStack.verifDirPath(homeDir)
        pubDirPath = ZStack.publicDirPath(homeDir)
        for d in (homeDir, verifDirPath, pubDirPath):
            os.makedirs(d, exist_ok=True)

        for other in others:
            createCertsFromKeys(verifDirPath, other.name, other.verKey)
            createCertsFromKeys(pubDirPath, other.name, other.publicKey)

    def tellKeysToOthers(self, others):
        for other in others:
            createCertsFromKeys(other.verifKeyDir, self.name, self.verKey)
            createCertsFromKeys(other.publicKeysDir, self.name, self.publicKey)

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
            logger.display("Signing and Encryption keys were not found for {}. Creating them now".
                           format(self), extra={"cli": False})
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
        self.auth = MultiZapAuthenticator(self.ctx)
        self.auth.start()
        self.auth.allow('0.0.0.0')
        self.auth.configure_curve(domain='*', location=location)

    def teardownAuth(self):
        if self.auth:
            self.auth.stop()

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
        # self.ctx = test.asyncio.Context.instance()
        if self.config.NEW_CTXT_INSTANCE:
            self.ctx = zmq.Context()
        else:
            self.ctx = zmq.Context.instance()
        if self.config.MAX_SOCKETS:
            self.ctx.MAX_SOCKETS = self.config.MAX_SOCKETS
        restricted = self.restricted if restricted is None else restricted
        logger.debug('{} starting with restricted as {} and reSetupAuth '
                     'as {}'.format(self, restricted, reSetupAuth),
                     extra={"cli": False, "demo": False})
        self.setupAuth(restricted, force=reSetupAuth)
        self.open()

    def stop(self):
        if self.opened:
            logger.display('stack {} closing its listener'.format(self), extra={"cli": False, "demo": False})
            self.close()
        logger.display("stack {} stopped".format(self), extra={"cli": False, "demo": False})

    @property
    def opened(self):
        return self.listener is not None

    def open(self):
        # noinspection PyUnresolvedReferences
        self.listener = self.ctx.socket(zmq.ROUTER)
        self._client_message_provider.listener = self.listener
        self.listener.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.listener.setsockopt(zmq.ROUTER_HANDOVER, self.config.ROUTER_HANDOVER)
        if self.create_listener_monitor:
            self.listener_monitor = self.listener.get_monitor_socket()
        # noinspection PyUnresolvedReferences
        # self.poller.register(self.listener, test.POLLIN)
        public, secret = self.selfEncKeys
        self.listener.curve_secretkey = secret
        self.listener.curve_publickey = public
        self.listener.curve_server = True
        self.listener.identity = self.publicKey
        logger.info(
            '{} will bind its listener at {}:{}'.format(self, self.ha[0], self.ha[1]))
        set_keepalive(self.listener, self.config)
        set_zmq_internal_queue_size(self.listener, self.queue_size)
        # Cycle to deal with "Address already in use" in case of immediate stack restart.
        bound = False

        sleep_between_bind_retries = 0.2
        bind_retry_time = 0
        while not bound:
            try:
                self.listener.bind(
                    '{protocol}://{ip}:{port}'.format(ip=self.ha[0], port=self.ha[1],
                                                      protocol=ZMQ_NETWORK_PROTOCOL)
                )
                bound = True
            except zmq.error.ZMQError as zmq_err:
                logger.warning("{} can not bind to {}:{}. Will try in {} secs.".
                               format(self, self.ha[0], self.ha[1], sleep_between_bind_retries))
                bind_retry_time += sleep_between_bind_retries
                if bind_retry_time > self.config.MAX_WAIT_FOR_BIND_SUCCESS:
                    logger.warning("{} can not bind to {}:{} for {} secs. Going to restart the service.".
                                   format(self, self.ha[0], self.ha[1], self.config.MAX_WAIT_FOR_BIND_SUCCESS))
                    raise zmq_err
                time.sleep(sleep_between_bind_retries)

    def close(self):
        if self.config.NEW_CTXT_INSTANCE:
            self.ctx.destroy(linger=0)
            self.listener = None
            self._remotes = {}
            self.remotesByKeys = {}
            self._conns = set()
        else:
            if self.listener_monitor is not None:
                self.listener_monitor.linger = 0
                self.listener.disable_monitor()
                self.listener_monitor = None
            self.listener.unbind(self.listener.LAST_ENDPOINT)
            self.listener.close(linger=0)
            self.listener = None
            logger.debug('{} starting to disconnect remotes'.format(self))
            for r in self.remotes.values():
                r.disconnect()
                self.remotesByKeys.pop(r.publicKey, None)

            self._remotes = {}
            if self.remotesByKeys:
                logger.debug('{} found remotes that were only in remotesByKeys and '
                             'not in remotes. This is suspicious')
                for r in self.remotesByKeys.values():
                    r.disconnect()
                self.remotesByKeys = {}
            self._conns = set()

            self.teardownAuth()

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
        # TODO: Change name after removing test
        return not self.isRestricted

    def isConnectedTo(self, name: str = None, ha: Tuple = None):
        return not self.onlyListener and super().isConnectedTo(name, ha)

    def hasRemote(self, name):
        return not self.onlyListener and super().hasRemote(name)

    def removeRemoteByName(self, name: str):
        return not self.onlyListener and super().removeRemoteByName(name)

    def getHa(self, name):
        # Return HA as None when its a `peersWithoutRemote`
        if self.onlyListener:
            return None
        return super().getHa(name)

    async def service(self, limit=None, quota: Optional[Quota] = None) -> int:
        """
        Service `limit` number of received messages in this stack.

        :param limit: the maximum number of messages to be processed. If None,
        processes all of the messages in rxMsgs.
        :return: the number of messages processed.
        """
        if self.listener:
            await self._serviceStack(self.age, quota)
        else:
            logger.info("{} is stopped".format(self))

        r = len(self.rxMsgs)
        if r > 0:
            pracLimit = limit if limit else sys.maxsize
            return self.processReceived(pracLimit)
        return 0

    def _verifyAndAppend(self, msg, ident):
        try:
            ident.decode()
        except ValueError:
            logger.error("Identifier {} is not decoded into UTF-8 string. "
                         "Request will not be processed".format(ident))
            return False
        try:
            self.metrics.add_event(self.mt_incoming_size, len(msg))
            self.msgLenVal.validate(msg)
            decoded = msg.decode()
        except (UnicodeDecodeError, InvalidMessageExceedingSizeException) as ex:
            errstr = 'Message will be discarded due to {}'.format(ex)
            frm = self.remotesByKeys[ident].name if ident in self.remotesByKeys else ident
            logger.error("Got from {} {}".format(z85_to_friendly(frm), errstr))
            self.msgRejectHandler(errstr, frm)
            return False
        self.rxMsgs.append((decoded, ident))
        return True

    def _receiveFromListener(self, quota: Quota) -> int:
        """
        Receives messages from listener
        :param quota: number of messages to receive
        :return: number of received messages
        """
        i = 0
        incoming_size = 0
        while i < quota.count and incoming_size < quota.size:
            try:
                received = self.listener.recv_multipart(flags=zmq.NOBLOCK)
                # If client was connected in DEALER mode then is expected
                # to get exactly 2 values from recv_multipart function
                if len(received) > 2:
                    logger.debug("Got too many values for unpack. "
                                 "It seems that client's socket is open not in DEALER mode")
                    continue
                ident, msg = received
                if not msg:
                    # Router probing sends empty message on connection
                    continue
                incoming_size += len(msg)
                i += 1
                self._verifyAndAppend(msg, ident)
            except zmq.Again as e:
                break
            except zmq.ZMQError as e:
                logger.debug("Strange ZMQ behaviour during node-to-node message receiving, experienced {}".format(e))
        if i > 0:
            logger.trace('{} got {} messages through listener'.
                         format(self, i))
        return i

    def _receiveFromRemotes(self, quotaPerRemote) -> int:
        """
        Receives messages from remotes
        :param quotaPerRemote: number of messages to receive from one remote
        :return: number of received messages
        """

        assert quotaPerRemote
        totalReceived = 0
        for ident, remote in self.remotesByKeys.items():
            if not remote.socket:
                continue
            i = 0
            sock = remote.socket
            while i < quotaPerRemote:
                try:
                    msg, = sock.recv_multipart(flags=zmq.NOBLOCK)
                    if not msg:
                        # Router probing sends empty message on connection
                        continue
                    i += 1
                    logger.trace("{} received a message from remote {} by socket {} {}".
                                 format(self, z85_to_friendly(ident), sock.FD, sock.underlying))
                    self._verifyAndAppend(msg, ident)
                except zmq.Again as e:
                    break
                except zmq.ZMQError as e:
                    logger.debug(
                        "Strange ZMQ behaviour during node-to-node message receiving, experienced {}".format(e))
            if i > 0:
                logger.trace('{} got {} messages through remote {}'.
                             format(self, i, remote))
            totalReceived += i
        return totalReceived

    async def _serviceStack(self, age, quota: Optional[Quota] = None):
        # TODO: age is unused

        # These checks are kept here and not moved to a function since
        # `_serviceStack` is called very often and function call is an overhead
        if self.config.ENABLE_HEARTBEATS and (
            self.last_heartbeat_at is None or
            (time.perf_counter() - self.last_heartbeat_at) >=
                self.config.HEARTBEAT_FREQ):
            self.send_heartbeats()

        if quota is None:
            quota = Quota(count=self.listenerQuota, size=self.listenerSize)

        self._receiveFromListener(quota)
        self._receiveFromRemotes(quotaPerRemote=self.senderQuota)
        return len(self.rxMsgs)

    def processReceived(self, limit):
        if limit <= 0:
            return 0
        num_processed = 0
        for num_processed in range(limit):
            if len(self.rxMsgs) == 0:
                return num_processed

            msg, ident = self.rxMsgs.popleft()
            frm = self.remotesByKeys[ident].name \
                if ident in self.remotesByKeys else ident

            if not self.config.RETRY_CONNECT and ident in self.remotesByKeys:
                self.remotesByKeys[ident].setConnected()

            if self.handlePingPong(msg, frm, ident):
                continue

            if not self.onlyListener and ident not in self.remotesByKeys:
                if self._node_mode != Mode.participating:
                    logger.info('{} not yet caught-up, stashing message from unknown remote'
                                .format(self, z85_to_friendly(ident)))
                    self._stashed_unknown_remote_msgs.append((msg, ident))
                else:
                    logger.warning('{} received message from unknown remote {}'
                                   .format(self, z85_to_friendly(ident)))
                continue

            try:
                msg = self.deserializeMsg(msg)
            except Exception as e:
                logger.error('Error {} while converting message {} '
                             'to JSON from {}'.format(e, msg, z85_to_friendly(ident)))
                continue
            # We have received non-ping-pong message from some remote, we can clean this counter
            if OP_FIELD_NAME not in msg or msg[OP_FIELD_NAME] != BATCH:
                self.remote_ping_stats[z85_to_friendly(frm)] = 0
            msg = self.doProcessReceived(msg, frm, ident)
            if msg:
                self.msgHandler((msg, frm))
        return num_processed + 1

    def doProcessReceived(self, msg, frm, ident):
        return msg

    def connect(self,
                name=None,
                remoteId=None,
                ha=None,
                verKeyRaw=None,
                publicKeyRaw=None):
        """
        Connect to the node specified by name.
        """
        if not name:
            raise ValueError('Remote name should be specified')

        publicKey = None
        if name in self.remotes:
            remote = self.remotes[name]
        else:
            publicKey = z85.encode(
                publicKeyRaw) if publicKeyRaw else self.getPublicKey(name)
            verKey = z85.encode(
                verKeyRaw) if verKeyRaw else self.getVerKey(name)
            if not ha or not publicKey or (self.isRestricted and not verKey):
                raise ValueError('{} doesnt have enough info to connect. '
                                 'Need ha, public key and verkey. {} {} {}'.
                                 format(name, ha, verKey, publicKey))
            remote = self.addRemote(name, ha, verKey, publicKey)

        public, secret = self.selfEncKeys
        remote.connect(self.ctx, public, secret)

        logger.info("{}{} looking for {} at {}:{}"
                    .format(CONNECTION_PREFIX, self,
                            name or remote.name, *remote.ha),
                    extra={"cli": "PLAIN", "tags": ["node-looking"]})

        # This should be scheduled as an async task

        self.sendPingPong(remote, is_ping=True)

        # re-send previously stashed pings/pongs from unknown remotes
        logger.trace("{} stashed pongs: {}".format(self.name, str(self._stashed_pongs)))
        if publicKey in self._stashed_pongs:
            logger.trace("{} sending stashed pongs to {}".format(self.name, str(z85_to_friendly(publicKey))))
            self._stashed_pongs.discard(publicKey)
            self.sendPingPong(name, is_ping=False)

        return remote.uid

    def reconnectRemote(self, remote):
        """
        Disconnect remote and connect to it again

        :param remote: instance of Remote from self.remotes
        :param remoteName: name of remote
        :return:
        """
        if not isinstance(remote, Remote):
            raise PlenumTypeError('remote', remote, Remote)
        logger.info('{} reconnecting to {}'.format(self, remote))
        public, secret = self.selfEncKeys
        remote.disconnect()
        remote.connect(self.ctx, public, secret)
        self.sendPingPong(remote, is_ping=True)

    def reconnectRemoteWithName(self, remoteName):
        if remoteName not in self.remotes:
            raise PlenumValueError(
                'remoteName', remoteName,
                "one of {}".format(self.remotes)
            )
        self.reconnectRemote(self.remotes[remoteName])

    def disconnectByName(self, remoteName: str):
        if not remoteName:
            raise PlenumValueError(
                'remoteName', remoteName,
                "non-empty string"
            )
        remote = self.remotes.get(remoteName)
        if not remote:
            logger.debug('{} did not find any remote '
                         'by name {} to disconnect'
                         .format(self, remoteName))
            return None
        remote.disconnect()
        return remote

    def addRemote(self, name, ha, remoteVerkey, remotePublicKey):
        if not name:
            raise PlenumValueError('name', name, 'non-empty')
        remote = self._RemoteClass(name, ha, remoteVerkey, remotePublicKey, self.queue_size, self.ha[0])
        self.remotes[name] = remote
        # TODO: Use weakref to remote below instead
        self.remotesByKeys[remotePublicKey] = remote
        if remoteVerkey:
            self.addVerifier(remoteVerkey)
        else:
            logger.display('{} adding a remote {}({}) without a verkey'.format(self, name, ha))
        return remote

    def sendPingPong(self, remote: Union[str, Remote], is_ping=True):
        msg = self.pingMessage if is_ping else self.pongMessage
        action = 'ping' if is_ping else 'pong'
        name = remote if isinstance(remote, (str, bytes)) else remote.name
        # Do not use Batches for sending health messages
        r = self.transmit(msg, name, is_batch=False)
        # r = self.send(msg, name)
        if r[0] is True:
            logger.debug('{} {}ed {}'.format(self.name, action, z85_to_friendly(name)))
        elif r[0] is False:
            logger.debug('{} failed to {} {} {}'
                         .format(self.name, action, z85_to_friendly(name), r[1]),
                         extra={"cli": False})
            # try to re-send pongs later
            if not is_ping:
                self._stashed_pongs.add(name)
        elif r[0] is None:
            logger.debug('{} will be sending in batch'.format(self))
        else:
            logger.error('{}{} got an unexpected return value {} while sending'.
                         format(CONNECTION_PREFIX, self, r))
        return r[0]

    def handlePingPong(self, msg, frm, ident):
        if msg in (self.pingMessage, self.pongMessage):
            if msg == self.pingMessage:
                nodeName = z85_to_friendly(frm)
                logger.trace('{} got ping from {}'.format(self, nodeName))
                self.sendPingPong(frm, is_ping=False)
                if not self.config.ENABLE_HEARTBEATS and self.config.PING_RECONNECT_ENABLED and nodeName in self.connecteds:
                    if self.remote_ping_stats.get(nodeName):
                        self.remote_ping_stats[nodeName] += 1
                    else:
                        self.remote_ping_stats[nodeName] = 1
                    if self.remote_ping_stats[nodeName] > self.config.PINGS_BEFORE_SOCKET_RECONNECTION:
                        logger.info("Reconnecting {} due to numerous consecutive pings".format(nodeName))
                        self.remote_ping_stats[nodeName] = 0
                        self.reconnectRemoteWithName(nodeName)
            if msg == self.pongMessage:
                if ident in self.remotesByKeys:
                    self.remotesByKeys[ident].setConnected()
                logger.trace('{} got pong from {}'.format(self, z85_to_friendly(frm)))
            return True
        return False

    def process_unknown_remote_msgs(self):
        logger.info('Processing messages from previously unknown remotes.')
        for msg in self._stashed_unknown_remote_msgs:
            self.rxMsgs.append(msg)

        self._stashed_unknown_remote_msgs.clear()
        self.processReceived(limit=sys.maxsize)

    def send_heartbeats(self):
        # Sends heartbeat (ping) to all
        logger.debug('{} sending heartbeat to all remotes'.format(self))
        for remote in self.remotes:
            self.sendPingPong(remote)
        self.last_heartbeat_at = time.perf_counter()

    def send(self, msg: Any, remoteName: str = None, ha=None):
        if self.onlyListener:
            return self._client_message_provider.transmit_through_listener(msg,
                                                                           remoteName)
        else:
            is_batch = isinstance(msg, Mapping) and msg.get(OP_FIELD_NAME) == BATCH
            if remoteName is None:
                r = []
                e = []
                # Serializing beforehand since to avoid serializing for each
                # remote
                try:
                    msg = self.prepare_to_send(msg)
                except InvalidMessageExceedingSizeException as ex:
                    err_str = '{}Cannot send message. Error {}'.format(CONNECTION_PREFIX, ex)
                    logger.warning(err_str)
                    return False, err_str
                for uid in self.remotes:
                    res, err = self.transmit(msg, uid, serialized=True, is_batch=is_batch)
                    r.append(res)
                    e.append(err)
                e = list(filter(lambda x: x is not None, e))
                ret_err = None if len(e) == 0 else "\n".join(e)
                return all(r), ret_err
            else:
                return self.transmit(msg, remoteName, is_batch=is_batch)

    def transmit(self, msg, uid, timeout=None, serialized=False, is_batch=False):
        remote = self.remotes.get(uid)
        err_str = None
        if not remote:
            logger.debug("Remote {} does not exist!".format(z85_to_friendly(uid)))
            return False, err_str
        socket = remote.socket
        if not socket:
            logger.debug('{} has uninitialised socket '
                         'for remote {}'.format(self, z85_to_friendly(uid)))
            return False, err_str
        try:
            if not serialized:
                msg = self.prepare_to_send(msg)

            logger.trace('{} transmitting message {} to {} by socket {} {}'
                         .format(self, msg, z85_to_friendly(uid), socket.FD, socket.underlying))
            socket.send(msg, flags=zmq.NOBLOCK)

            if remote.isConnected or msg in self.healthMessages:
                self.metrics.add_event(self.mt_outgoing_size, len(msg))
            else:
                logger.debug('Remote {} is not connected - message will not be sent immediately.'
                             'If this problem does not resolve itself - check your firewall settings'
                             .format(z85_to_friendly(uid)))

            return True, err_str
        except zmq.Again:
            logger.warning('{} could not transmit message to {}'.format(self, z85_to_friendly(uid)))
        except InvalidMessageExceedingSizeException as ex:
            err_str = '{}Cannot transmit message. Error {}'.format(CONNECTION_PREFIX, ex)
            logger.warning(err_str)
        return False, err_str

    @staticmethod
    def serializeMsg(msg):
        if isinstance(msg, Mapping):
            msg = json.dumps(msg)
        if isinstance(msg, str):
            msg = msg.encode()
        assert isinstance(msg, bytes)
        return msg

    @staticmethod
    def deserializeMsg(msg):
        if isinstance(msg, bytes):
            msg = msg.decode()
        msg = json.loads(msg)
        return msg

    def signedMsg(self, msg: bytes, signer: Signer = None):
        sig = self.signer.signature(msg)
        return msg + sig

    def verify(self, msg, by):
        if self.isKeySharing:
            return True
        if by not in self.remotesByKeys:
            return False
        verKey = self.remotesByKeys[by].verKey
        r = self.verifiers[verKey].verify(
            msg[-self.sigLen:], msg[:-self.sigLen])
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

    @property
    def publicKeyRaw(self):
        return z85.decode(self.publicKey)

    @property
    def pubhex(self):
        return hexlify(z85.decode(self.publicKey))

    def getPublicKey(self, name):
        try:
            return self.loadPubKeyFromDisk(self.publicKeysDir, name)
        except KeyError:
            raise PublicKeyNotFoundOnDisk(self.name, name)

    @property
    def verKey(self):
        return self.getVerKey(self.name)

    @property
    def verKeyRaw(self):
        if self.verKey:
            return z85.decode(self.verKey)
        return None

    @property
    def verhex(self):
        if self.verKey:
            return hexlify(z85.decode(self.verKey))
        return None

    def getVerKey(self, name):
        try:
            return self.loadPubKeyFromDisk(self.verifKeyDir, name)
        except KeyError:
            if self.isRestricted:
                raise VerKeyNotFoundOnDisk(self.name, name)
            return None

    @property
    def sigKey(self):
        return self.loadSecKeyFromDisk(self.sigKeyDir, self.name)

    # TODO: Change name to sighex after removing test
    @property
    def keyhex(self):
        return hexlify(z85.decode(self.sigKey))

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
            logger.debug('{} setting restricted to {}'.
                         format(self, restricted))
            self.stop()

            # TODO: REMOVE, it will make code slow, only doing to allow the
            # socket to become available again
            time.sleep(1)

            self.start(restricted, reSetupAuth=True)

    def _safeRemove(self, filePath):
        try:
            os.remove(filePath)
        except Exception as ex:
            logger.info('{} could delete file {} due to {}'.format(self, filePath, ex))

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

    def prepare_to_send(self, msg: Any):
        msg_bytes = self.serializeMsg(msg)
        return msg_bytes

    @staticmethod
    def get_monitor_events(monitor_socket, non_block=True):
        events = []
        # noinspection PyUnresolvedReferences
        flags = zmq.NOBLOCK if non_block else 0
        while True:
            try:
                # noinspection PyUnresolvedReferences
                message = recv_monitor_message(monitor_socket, flags)
                events.append(message)
            except zmq.Again:
                break
        return events


class DummyKeep:
    def __init__(self, stack, *args, **kwargs):
        self.stack = stack
        self._auto = 2 if stack.isKeySharing else 0

    @property
    def auto(self):
        logger.debug('{} proxy method used on {}'.
                     format(inspect.stack()[0][3], self))
        return self._auto

    @auto.setter
    def auto(self, mode):
        logger.debug('{} proxy method used on {}'.
                     format(inspect.stack()[0][3], self))
        # AutoMode.once whose value is 1 is not used os dont care
        if mode != self._auto:
            if mode == 2:
                self.stack.setRestricted(False)
            if mode == 0:
                self.stack.setRestricted(True)
