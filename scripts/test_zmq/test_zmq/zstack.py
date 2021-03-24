from test_zmq.authenticator import MultiZapAuthenticator

try:
    import ujson as json
except ImportError:
    import json

import os
import shutil
import sys
import time
from binascii import hexlify, unhexlify
from collections import deque
from typing import Mapping, Tuple, Any, Union, Optional, NamedTuple

import zmq.auth
from zmq.utils import z85
from zmq.utils.monitor import recv_monitor_message

import zmq
from .util import createEncAndSigKeys, \
    moveKeyFilesToCorrectLocations, createCertsFromKeys
from test_zmq.crypto.nacl_wrappers import Signer, Verifier
from test_zmq.crypto.util import isHex, ed25519PkToCurve25519

Quota = NamedTuple("Quota", [("count", int), ("size", int)])
ZMQ_NETWORK_PROTOCOL = 'tcp'


class ClientMessageProvider:
    def __init__(self, name, prepare_to_send, mt_outgoing_size, listener=None):
        self._name = name
        self.listener = listener
        self._prepare_to_send = prepare_to_send
        self._mt_outgoing_size = mt_outgoing_size

    def transmit_through_listener(self, msg, ident) -> Tuple[bool, Optional[str]]:
        result, error_msg, need_to_resend = self._transmit_one_msg_throughlistener(msg,
                                                                                   ident)
        return result, error_msg

    def _transmit_one_msg_throughlistener(self, msg, ident) -> Tuple[bool, Optional[str], bool]:

        def prepare_error_msg(ex):
            err_str = '{} got error {} while sending through listener to {}' \
                .format(self, ex, ident)
            print(err_str)
            return err_str

        need_to_resend = False
        if isinstance(ident, str):
            ident = ident.encode()
        try:
            msg = self._prepare_to_send(msg)
            self.listener.send_multipart([ident, msg], flags=zmq.NOBLOCK)
        except zmq.Again as ex:
            need_to_resend = True
            return False, prepare_error_msg(ex), need_to_resend
        except zmq.ZMQError as ex:
            need_to_resend = (ex.errno == 113)
            return False, prepare_error_msg(ex), need_to_resend
        except Exception as ex:
            return False, prepare_error_msg(ex), need_to_resend
        return True, None, need_to_resend

    def __repr__(self):
        return self._name


# TODO: Use Async io
# TODO: There a number of methods related to keys management,
# they can be moved to some class like KeysManager
class ZStack():
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

    def __init__(self, name, ha, basedirpath, msgHandler, restricted=True,
                 seed=None, onlyListener=False, config=None, msgRejectHandler=None, queue_size=0,
                 create_listener_monitor=False,
                 mt_incoming_size=None, mt_outgoing_size=None, timer=None):
        self._name = name
        self.ha = ha
        self.basedirpath = basedirpath
        self.msgHandler = msgHandler
        self.seed = seed
        self.queue_size = queue_size
        self.msgRejectHandler = msgRejectHandler or self.__defaultMsgRejectHandler

        self._node_mode = None
        self._stashed_unknown_remote_msgs = deque()

        self.mt_incoming_size = mt_incoming_size
        self.mt_outgoing_size = mt_outgoing_size

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
        self._remotes = {}

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
                                                              self.prepare_to_send,
                                                              self.mt_outgoing_size)

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
            print("Signing and Encryption keys were not found for {}. Creating them now".format(self))
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
        self.ctx = zmq.Context()
        restricted = self.restricted if restricted is None else restricted
        print('{} starting with restricted as {} and reSetupAuth '
              'as {}'.format(self, restricted, reSetupAuth))
        self.setupAuth(restricted, force=reSetupAuth)
        self.open()

    def stop(self):
        if self.opened:
            print('stack {} closing its listener'.format(self))
            self.close()
        print("stack {} stopped".format(self))

    @property
    def opened(self):
        return self.listener is not None

    def open(self):
        # noinspection PyUnresolvedReferences
        self.listener = self.ctx.socket(zmq.ROUTER)
        self._client_message_provider.listener = self.listener
        self.listener.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.listener.setsockopt(zmq.ROUTER_HANDOVER, 1)
        if self.create_listener_monitor:
            self.listener_monitor = self.listener.get_monitor_socket()
        # noinspection PyUnresolvedReferences
        # self.poller.register(self.listener, test.POLLIN)
        public, secret = self.selfEncKeys
        self.listener.curve_secretkey = secret
        self.listener.curve_publickey = public
        self.listener.curve_server = True
        self.listener.identity = self.publicKey
        print('{} will bind its listener at {}:{}'.format(self, self.ha[0], self.ha[1]))
        self.listener.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.listener.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
        self.listener.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 20)
        self.listener.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        self.listener.set_hwm(0)
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
                print("{} can not bind to {}:{}. Will try in {} secs.".
                      format(self, self.ha[0], self.ha[1], sleep_between_bind_retries))
                bind_retry_time += sleep_between_bind_retries
                if bind_retry_time > self.config.MAX_WAIT_FOR_BIND_SUCCESS:
                    raise zmq_err
                time.sleep(sleep_between_bind_retries)

    def close(self):
        if self.listener_monitor is not None:
            self.listener.disable_monitor()
            self.listener_monitor = None
        self.listener.unbind(self.listener.LAST_ENDPOINT)
        self.listener.close(linger=0)
        self.listener = None
        print('{} starting to disconnect remotes'.format(self))
        for r in self.remotes.values():
            r.disconnect()
            self.remotesByKeys.pop(r.publicKey, None)

        self._remotes = {}
        if self.remotesByKeys:
            print('{} found remotes that were only in remotesByKeys and '
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
            await self._serviceStack(quota)
        else:
            print("{} is stopped".format(self))

        r = len(self.rxMsgs)
        if r > 0:
            pracLimit = limit if limit else sys.maxsize
            return self.processReceived(pracLimit)
        return 0

    def _receiveFromListener(self, quota: Quota) -> int:
        """
        Receives messages from listener
        :param quota: number of messages to receive
        :return: number of received messages
        """
        i = 0
        incoming_size = 0
        try:
            ident, msg = self.listener.recv_multipart(flags=zmq.NOBLOCK)
            if msg:
                # Router probing sends empty message on connection
                incoming_size += len(msg)
                i += 1
                self._verifyAndAppend(msg, ident)
        except zmq.Again as e:
            return i
        except zmq.ZMQError as e:
            print("Strange ZMQ behaviour during node-to-node message receiving, experienced {}".format(e))
        if i > 0:
            print('{} got {} messages through listener'.
                  format(self, i))
        return i

    def _verifyAndAppend(self, msg, ident):
        try:
            ident.decode()
        except ValueError:
            print("Identifier {} is not decoded into UTF-8 string. "
                  "Request will not be processed".format(ident))
            return False
        try:
            decoded = msg.decode()
        except (UnicodeDecodeError) as ex:
            errstr = 'Message will be discarded due to {}'.format(ex)
            frm = self.remotesByKeys[ident].name if ident in self.remotesByKeys else ident
            print("Got from {} {}".format(frm, errstr))
            self.msgRejectHandler(errstr, frm)
            return False
        self.rxMsgs.append((decoded, ident))
        return True

    async def _serviceStack(self, quota: Optional[Quota] = None):

        self._receiveFromListener(quota)
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
            self.msgHandler(self, (msg, frm))

        return num_processed + 1

    def doProcessReceived(self, msg, frm, ident):
        return msg

    def send(self, msg: Any, remoteName: str = None, ha=None):
        if self.onlyListener:
            return self._client_message_provider.transmit_through_listener(msg,
                                                                           remoteName)

    def transmit(self, msg, uid, timeout=None, serialized=False, is_batch=False):
        remote = self.remotes.get(uid)
        err_str = None
        if not remote:
            return False, err_str
        socket = remote.socket
        if not socket:
            return False, err_str
        try:
            if not serialized:
                msg = self.prepare_to_send(msg)

            print('{} transmitting message {} to {} by socket {} {}'
                  .format(self, msg, uid, socket.FD, socket.underlying))
            socket.send(msg, flags=zmq.NOBLOCK)

            return True, err_str
        except zmq.Again:
            print('{} could not transmit message to {}'.format(self, uid))
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
        return self.loadPubKeyFromDisk(self.publicKeysDir, name)

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
        return self.loadPubKeyFromDisk(self.verifKeyDir, name)

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
            print('{} setting restricted to {}'.
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
            print('{} could delete file {} due to {}'.format(self, filePath, ex))

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
