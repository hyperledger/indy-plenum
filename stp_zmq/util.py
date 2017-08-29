# TODO: Contains duplicated code, need to be refactored


import datetime
import os
import shutil

from libnacl import crypto_sign_seed_keypair
from zmq.auth.certs import _write_key_file, _cert_public_banner, \
    _cert_secret_banner
from zmq.utils import z85

from stp_core.crypto.util import ed25519PkToCurve25519 as ep2c, \
    ed25519SkToCurve25519 as es2c, randomSeed


def createCertsFromKeys(key_dir, name, public_key, secret_key=None,
                        metadata=None, pSuffix='key', sSuffix='key_secret'):
    public_key_file, secret_key_file = _get_key_files_paths(key_dir, name,
                                                            pSuffix, sSuffix)

    _write_secret_public_keys(public_key_file, secret_key_file,
                              public_key, secret_key,
                              metadata
                              )
    return public_key_file, secret_key_file


def _get_key_files_paths(key_dir, name, pSuffix, sSuffix):
    base_filename = os.path.join(key_dir, name)
    secret_key_file = "{}.{}".format(base_filename, sSuffix)
    public_key_file = "{}.{}".format(base_filename, pSuffix)
    return public_key_file, secret_key_file


def _write_secret_public_keys(public_key_file_path, secret_key_file_path, public_key,
                              secret_key, metadata):
    current_time = datetime.datetime.now()
    _write_public_key_file(public_key_file_path, current_time, public_key)
    _write_secret_key_file(secret_key_file_path, current_time, public_key,
                           secret_key, metadata)


def _write_public_key_file(key_filename, current_time, public_key):
    banner = _cert_public_banner.format(current_time)
    _create_file_with_mode(key_filename, 0o644)
    _write_key_file(key_filename,
                    banner,
                    public_key,
                    secret_key=None,
                    metadata=None,
                    encoding='utf-8')


def _write_secret_key_file(key_filename, current_time,
                           public_key, secret_key, metadata):
    banner = _cert_secret_banner.format(current_time)
    _create_file_with_mode(key_filename, 0o600)
    _write_key_file(key_filename,
                    banner,
                    public_key,
                    secret_key=secret_key,
                    metadata=metadata,
                    encoding='utf-8')


def _create_file_with_mode(path, mode):
    open(path, 'a').close()
    os.chmod(path, mode)


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
