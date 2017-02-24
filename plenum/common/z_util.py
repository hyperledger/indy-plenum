import os
import shutil
import datetime

from libnacl import crypto_sign_seed_keypair
from zmq.auth.certs import _write_key_file, _cert_public_banner, \
    _cert_secret_banner
from zmq.utils import z85
from plenum.common.crypto import ed25519PkToCurve25519 as ep2c, \
    ed25519SkToCurve25519 as es2c
from plenum.common.util import randomSeed


def createCertsFromKeys(key_dir, name, public_key, secret_key=None,
                        metadata=None, pSuffix='key', sSuffix='key_secret'):
    base_filename = os.path.join(key_dir, name)
    secret_key_file = "{}.{}".format(base_filename, sSuffix)
    public_key_file = "{}.{}".format(base_filename, pSuffix)
    now = datetime.datetime.now()

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
    verif_key, sig_key = crypto_sign_seed_keypair(seed)
    createCertsFromKeys(sig_key_dir, name, z85.encode(verif_key),
                        z85.encode(sig_key))
    public_key, secret_key = ep2c(verif_key), es2c(sig_key)
    createCertsFromKeys(enc_key_dir, name, z85.encode(public_key),
                        z85.encode(secret_key))


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

    print('Public keys in {}'.format(public_keys_dir))
    print('Private keys in {}'.format(secret_keys_dir))
    print('Verification keys in {}'.format(ver_keys_dir))
    print('Signing keys in {}'.format(sig_keys_dir))
