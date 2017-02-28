import os
import shutil
import datetime
from binascii import unhexlify, hexlify

from libnacl import crypto_sign_seed_keypair
from zmq.auth.certs import _write_key_file, _cert_public_banner, \
    _cert_secret_banner
from zmq.utils import z85
from plenum.common.crypto import ed25519PkToCurve25519 as ep2c, \
    ed25519SkToCurve25519 as es2c
from plenum.common.types import CLIENT_STACK_SUFFIX
from plenum.common.util import randomSeed, isHex


# TODO: Contains duplicated code, need to be refactored


def createCertsFromKeys(key_dir, name, public_key, secret_key=None,
                        metadata=None, pSuffix='key', sSuffix='key_secret'):
    base_filename = os.path.join(key_dir, name)
    secret_key_file = "{}.{}".format(base_filename, sSuffix)
    public_key_file = "{}.{}".format(base_filename, pSuffix)
    now = datetime.datetime.now()
    print('{} writing {} {} in {}'.format(name, public_key, secret_key, key_dir))
    _write_key_file(public_key_file,
                    _cert_public_banner.format(now),
                    public_key)

    _write_key_file(secret_key_file,
                    _cert_secret_banner.format(now),
                    public_key,
                    secret_key=secret_key,
                    metadata=metadata)

    return public_key_file, secret_key_file


def createEncAndSigKeys(enc_key_dir, sig_key_dir, name, seed=None):
    seed = seed or randomSeed()
    if isinstance(seed, str):
        seed = seed.encode()
    # ATTENTION: Passing `seed` encoded to bytes or not in
    # `crypto_sign_seed_keypair` will generate different keypairs
    verif_key, sig_key = crypto_sign_seed_keypair(seed)
    createCertsFromKeys(sig_key_dir, name, z85.encode(verif_key),
                        z85.encode(sig_key[:32]))
    public_key, secret_key = ep2c(verif_key), es2c(sig_key)
    createCertsFromKeys(enc_key_dir, name, z85.encode(public_key),
                        z85.encode(secret_key))
    return (public_key, secret_key), (verif_key, sig_key)


def moveKeyFilesToCorrectLocations(keys_dir, pkdir, skdir):
    for key_file in os.listdir(keys_dir):
        if key_file.endswith(".key"):
            try:
                shutil.move(os.path.join(keys_dir, key_file),
                            os.path.join(pkdir, key_file))
            except shutil.Error as ex:
                # print(ex)
                pass
        if key_file.endswith(".key_secret"):
            try:
                shutil.move(os.path.join(keys_dir, key_file),
                            os.path.join(skdir, key_file))
            except shutil.Error as ex:
                # print(ex)
                pass


def generate_certificates(base_dir, *peer_names, pubKeyDir=None,
                          secKeyDir=None, sigKeyDir=None,
                          verkeyDir=None, clean=True):
    ''' Generate client and server CURVE certificate files'''
    pubKeyDir = pubKeyDir or 'public_keys'
    secKeyDir = secKeyDir or 'private_keys'
    verkeyDir = verkeyDir or 'verif_keys'
    sigKeyDir = sigKeyDir or 'sig_keys'

    # keys_dir = os.path.join(base_dir, 'certificates')
    e_keys_dir = os.path.join(base_dir, '_enc')
    s_keys_dir = os.path.join(base_dir, '_sig')

    public_keys_dir = os.path.join(base_dir, pubKeyDir)
    secret_keys_dir = os.path.join(base_dir, secKeyDir)
    ver_keys_dir = os.path.join(base_dir, verkeyDir)
    sig_keys_dir = os.path.join(base_dir, sigKeyDir)

    # Create directories for certificates, remove old content if necessary
    for d in [e_keys_dir, s_keys_dir, public_keys_dir, secret_keys_dir,
              ver_keys_dir, sig_keys_dir]:
        if clean and os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    # create new keys in certificates dir
    for peer_name in peer_names:
        createEncAndSigKeys(e_keys_dir, s_keys_dir, peer_name)

    # move public keys to appropriate directory
    for keys_dir, pkdir, skdir in [
        (e_keys_dir, public_keys_dir, secret_keys_dir),
        (s_keys_dir, ver_keys_dir, sig_keys_dir)
    ]:
        moveKeyFilesToCorrectLocations(keys_dir, pkdir, skdir)

    shutil.rmtree(e_keys_dir)
    shutil.rmtree(s_keys_dir)

    print('Public keys in {}'.format(public_keys_dir))
    print('Private keys in {}'.format(secret_keys_dir))
    print('Verification keys in {}'.format(ver_keys_dir))
    print('Signing keys in {}'.format(sig_keys_dir))


def initStackLocalKeys(name, baseDir, sigseed, override=False):
    # TODO: Implement override functionality
    sDir = os.path.join(baseDir, '__sDir')
    eDir = os.path.join(baseDir, '__eDir')
    os.makedirs(sDir, exist_ok=True)
    os.makedirs(eDir, exist_ok=True)
    (public_key, secret_key), (verif_key, sig_key) = createEncAndSigKeys(eDir,
                                                                         sDir,
                                                                         name,
                                                                         seed=sigseed)

    from plenum.common.zstack import ZStack
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


def initNodeKeysForBothStacks(name, baseDir, sigseed, override=False):
    initStackLocalKeys(name, baseDir, sigseed, override=override)
    initStackLocalKeys(name + CLIENT_STACK_SUFFIX, baseDir, sigseed,
                       override=override)


def initRemoteKeys(name, remoteName, baseDir, verkey, override=False):
    # TODO: Implement override functionality

    from plenum.common.zstack import ZStack
    homeDir = ZStack.homeDirPath(baseDir, name)
    verifDirPath = ZStack.verifDirPath(homeDir)
    pubDirPath = ZStack.publicDirPath(homeDir)
    for d in (homeDir, verifDirPath, pubDirPath):
        os.makedirs(d, exist_ok=True)

    if isHex(verkey):
        verkey = unhexlify(verkey)

    createCertsFromKeys(verifDirPath, remoteName, z85.encode(verkey))
    public_key = ep2c(verkey)
    createCertsFromKeys(pubDirPath, remoteName, z85.encode(public_key))
