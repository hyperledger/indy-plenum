import json
from typing import Dict, Mapping
import os
import sys

import zmq
import zmq.asyncio
import zmq.auth
from raet.nacling import Signer, Verifier
from zmq.asyncio import Context
from zmq.utils import z85

from plenum.common.authenticator import AsyncioAuthenticator
from plenum.common.log import getlogger
from plenum.common.network_interface import NetworkInterface


logger = getlogger()

LINGER_TIME = 20


class Remote:
    def __init__(self, name, ha, verKey, publicKey, *args, **kwargs):
        # Every remote has a unique name per stack, the name can be the
        # public key of the other end
        self.name = name
        self.ha = ha
        # self.publicKey is the public key of the other end of the remote
        self.publicKey = publicKey
        # self.verKey is the verification key of the other end of the remote
        self.verKey = verKey
        self.socket = None
        self.isConnected = False
        # Currently keeping uid field to resemble RAET RemoteEstate
        self.uid = name

    def connect(self, context, localPubKey, localSecKey, typ=None):
        typ = typ or zmq.DEALER
        sock = context.socket(typ)
        sock.setsockopt(zmq.LINGER, LINGER_TIME)
        sock.curve_publickey = localPubKey
        sock.curve_secretkey = localSecKey
        sock.curve_serverkey = self.publicKey
        sock.identity = localPubKey
        addr = 'tcp://{}:{}'.format(*self.ha)
        sock.connect(addr)
        self.socket = sock

    def disconnect(self):
        self.socket.close()
        self.socket = None
        self.isConnected = False

    def __repr__(self):
        return '{}:{}'.format(self.name, self.ha)


class ZStack(NetworkInterface):
    # Assuming only one listener per stack for now.

    PublicKeyDirName = 'public_keys'
    PrivateKeyDirName = 'private_keys'
    VerifKeyDirName = 'verif_keys'
    SigKeyDirName = 'sig_keys'

    __sigKey__ = '__sig__'
    sigLen = 64
    pingMessage = b'\n'

    def __init__(self, name, ha, basedirpath, msgHandler, restricted=True):
        self.name = name
        self.ha = ha
        self.basedirpath = basedirpath
        self.msgHandler = msgHandler
        self.homeDir = None
        # As of now there would be only one file in secretKeysDir and sigKeyDir
        self.publicKeysDir = None
        self.secretKeysDir = None
        self.verifKeyDir = None
        self.sigKeyDir = None

        self.signer = None
        self.verifiers = {}

        self.setupDirs()
        self.setupSigning()

        self.poller = zmq.asyncio.Poller()
        self.restricted = restricted

        self.ctx = None  # type: Context
        self.listener = None
        self.auth = None

        # Each remote is identified uniquely by the name
        self.remotes = {}  # type: Dict[str, Remote]

        self.remotesByKeys = {}

    @staticmethod
    def keyDirNames():
        return ZStack.PublicKeyDirName, ZStack.PrivateKeyDirName, ZStack.VerifKeyDirName, ZStack.SigKeyDirName

    def __repr__(self):
        return self.name

    def setupDirs(self):
        self.homeDir = os.path.join(self.basedirpath, self.name)
        self.publicKeysDir = os.path.join(self.homeDir,
                                          self.PublicKeyDirName)
        self.secretKeysDir = os.path.join(self.homeDir,
                                          self.PrivateKeyDirName)
        self.verifKeyDir = os.path.join(self.homeDir,
                                        self.VerifKeyDirName)
        self.sigKeyDir = os.path.join(self.homeDir,
                                      self.SigKeyDirName)

        for d in (self.homeDir, self.publicKeysDir, self.secretKeysDir,
                  self.verifKeyDir, self.sigKeyDir):
            os.makedirs(d, exist_ok=True)

    def setupAuth(self, restricted=True, force=False):
        if self.listener and not force:
            raise RuntimeError('Listener already setup')
        location = self.publicKeysDir if restricted else zmq.auth.CURVE_ALLOW_ANY
        self.auth = AsyncioAuthenticator(self.ctx)
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

    def start(self, restricted=None):
        self.ctx = zmq.asyncio.Context.instance()
        restricted = self.restricted if restricted is None else restricted
        self.setupAuth(restricted)
        self.open()

    def stop(self):
        if self.opened:
            self.close()
        # TODO: Uncommenting this stops hangs the code, setting LINGER_TIME
        # does not help. Find a solution
        # self.ctx.term()
        logger.info("stack {} stopped".format(self.name), extra={"cli": False})

    def removeRemote(self, remote: Remote, clear=True):
        """
        Currently not using clear
        """
        name = remote.name
        key = remote.publicKey
        if name in self.remotes:
            self.remotes.pop(name)
            self.remotesByKeys.pop(key)
        else:
            logger.warn('No remote named {} present')

    @property
    def opened(self):
        return self.listener is not None

    def open(self):
        self.listener = self.ctx.socket(zmq.ROUTER)
        self.listener.setsockopt(zmq.LINGER, LINGER_TIME)
        self.poller.register(self.listener, zmq.POLLIN)
        public, secret = self.selfEncKeys
        self.listener.curve_secretkey = secret
        self.listener.curve_publickey = public
        self.listener.curve_server = True
        self.listener.bind(
            'tcp://*:{}'.format(self.ha[1]))

    def close(self):
        self.listener.close()
        self.listener = None
        for r in self.remotes.values():
            r.disconnect()

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
    def isKeySharing(self):
        # Change name after removing raet
        restricted = not self.auth.allow_any if self.auth is not None \
            else self.restricted
        return not restricted

    @staticmethod
    def isRemoteConnected(r: Remote) -> bool:
        """
        A node is considered to be connected if it is joined, allowed and alived.

        :param r: the remote to check
        """
        return r.isConnected

    async def service(self, limit=None) -> int:
        """
        Service `limit` number of received messages in this stack.

        :param limit: the maximum number of messages to be processed. If None,
        processes all of the messages in rxMsgs.
        :return: the number of messages processed.
        """
        pracLimit = limit if limit else sys.maxsize
        if self.listener:
            x = 0
            for x in range(pracLimit):
                try:
                    ident, msg = await self.listener.recv_multipart(
                        flags=zmq.NOBLOCK)
                    if self.verify(msg, ident):
                        msg = msg[:-self.sigLen]
                        if ident in self.remotesByKeys:
                            self.remotesByKeys[ident].isConnected = True
                        if msg == self.pingMessage:
                            continue
                        try:
                            msg = json.loads(msg.decode())
                        except Exception as e:
                            logger.error('Error while converting message {} '
                                         'to JSON from {}'.format(msg, ident))
                            continue
                        self.msgHandler((msg, ident))
                    else:
                        logger.error('Error while verifying message {} from {}'
                                     .format(msg, ident))
                except zmq.Again:
                    break
            return x
        else:
            logger.debug("{} is stopped".format(self))
            return 0

    def connect(self, name, ha=None, verKey=None, publicKey=None):
        """
        Connect to the node specified by name.
        """

        if name not in self.remotes:
            if not (ha and publicKey and verKey):
                raise ValueError('{} doesnt know {}. Pass ha, public key '
                                 'and verkey'.format(ha, publicKey))
            remote = self.addRemote(name, ha, verKey, publicKey)
        else:
            remote = self.remotes[name]

        public, secret = self.selfEncKeys
        remote.connect(self.ctx, public, secret)

        logger.info("{} looking for {} at {}:{}".
                    format(self, name or remote.name, *remote.ha),
                    extra={"cli": "PLAIN"})

        r = self.send(self.pingMessage, remote.name)
        if r:
            logger.debug('Pinged {} at {}'.format(self.name, self.ha))
        else:
            logger.warn('Failed to ping {} at {}'.format(self.name, self.ha))
        return remote.uid

    def addRemote(self, name, ha, remoteVerkey, remotePublicKey):
        remote = Remote(name, ha, remoteVerkey, remotePublicKey)
        self.remotes[name] = remote
        # TODO: Use weakref to remote below instead
        self.remotesByKeys[remotePublicKey] = remote
        return remote

    def send(self, msg, remote: str = None):
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
        if isinstance(msg, Mapping):
            msg = json.dumps(msg)
        if isinstance(msg, str):
            msg = msg.encode()
        assert isinstance(msg, bytes)

        try:
            socket.send(self.signedMsg(msg), flags=zmq.NOBLOCK)
            return True
        except zmq.Again:
            return False

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

    @property
    def publicKey(self):
        return self.getPublicKey(self.name)

    def getPublicKey(self, name):
        serverPublicFile = os.path.join(self.publicKeysDir,
                                        "{}.key".format(name))
        publicKey, _ = zmq.auth.load_certificate(serverPublicFile)
        return publicKey

    @property
    def verKey(self):
        return self.getVerKey(self.name)

    def getVerKey(self, name):
        serverVerifFile = os.path.join(self.verifKeyDir,
                                       "{}.key".format(name))
        verkey, _ = zmq.auth.load_certificate(serverVerifFile)
        return verkey

    def getAllVerKeys(self):
        keys = []
        for key_file in os.listdir(self.verifKeyDir):
            if key_file.endswith(".key"):
                serverVerifFile = os.path.join(self.verifKeyDir,
                                               key_file)
                serverPublic, _ = zmq.auth.load_certificate(serverVerifFile)
                keys.append(serverPublic)
        return keys

    # def addListener(self, ha):
    #     pass
    #
    # @property
    # def defaultListener(self):
    #     pass


class KITZStack(ZStack):
    pass
