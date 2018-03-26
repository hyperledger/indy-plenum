import pytest
import os
import stat
import jsonpickle

from stp_core.common.log import getlogger
from plenum.client.wallet import Wallet, WalletStorageHelper

logger = getlogger()

DEFAULT_DMODE = 0o700
DEFAULT_FMODE = 0o600
NOT_LISTED_PERMISSION = stat.S_IWOTH


def encode_wallet(wallet):
    return jsonpickle.encode(wallet, keys=True)


def decode_wallet(wdata):
    return jsonpickle.decode(wdata, keys=True)


def set_permissions(path, mode):
    os.chmod(path, mode)
    return stat.S_IMODE(os.stat(path).st_mode)


def get_permissions(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def check_permissions(path, mode):
    assert get_permissions(path) == mode


@pytest.fixture(scope='function')
def tdir_hierarchy(tdir_for_func):
    tdir_for_func = os.path.join(tdir_for_func, 'root')
    dirs = ['1/2/3', 'a/b/c']
    files = ['1/2/3/1.txt', 'a/2.txt', 'a/b/3.txt']
    for d in dirs:
        os.makedirs(os.path.join(tdir_for_func, d))
    for f in files:
        open(os.path.join(tdir_for_func, f), 'a').close()

    # switch off test permission
    for path in dirs + files:
        path = os.path.join(tdir_for_func, path)
        mode = get_permissions(path)
        if mode & NOT_LISTED_PERMISSION:
            set_permissions(path, mode & ~NOT_LISTED_PERMISSION)

    return (tdir_for_func, dirs, files)


@pytest.fixture(scope='function')
def keyrings_base_dir(tdir_for_func):
    return os.path.join(tdir_for_func, 'keyrings')


@pytest.fixture(scope='function')
def test_wallet():
    return Wallet("TestWallet")


def test_keyring_base_dir_new_permissions(tdir_for_func):
    # default
    keyringsBaseDir = os.path.join(tdir_for_func, 'keyrings')
    WalletStorageHelper(keyringsBaseDir)
    check_permissions(keyringsBaseDir, DEFAULT_DMODE)

    # non-default
    dmode = DEFAULT_DMODE + 1
    keyringsBaseDir = os.path.join(tdir_for_func, 'keyrings2')
    WalletStorageHelper(keyringsBaseDir, dmode=dmode)
    check_permissions(keyringsBaseDir, dmode)


def test_keyring_base_dir_exists_as_file(tdir_hierarchy):
    root, dirs, files = tdir_hierarchy
    with pytest.raises(NotADirectoryError):
        WalletStorageHelper(os.path.join(root, files[0]))


def test_keyring_base_dir_exists_as_dir(tdir_hierarchy):
    root, dirs, files = tdir_hierarchy
    dpath = os.path.join(root, dirs[0])
    mode1 = get_permissions(dpath)
    mode2 = mode1 | NOT_LISTED_PERMISSION
    WalletStorageHelper(dpath, dmode=mode2)
    check_permissions(dpath, mode2)


def test_store_wallet_by_empty_path_fail(
        tdir_for_func, keyrings_base_dir, test_wallet):
    wsh = WalletStorageHelper(keyrings_base_dir)

    for path in (None, ''):
        with pytest.raises(ValueError) as exc_info:
            wsh.saveWallet(test_wallet, path)

        exc_info.match(r'empty path')


def test_store_wallet_outside_fail(
        tdir_for_func, keyrings_base_dir, test_wallet):
    wsh = WalletStorageHelper(keyrings_base_dir)

    inv_paths = [
        os.path.join(keyrings_base_dir, '../wallet'),
        '../wallet',
        'a/../../wallet'
    ]

    #  docs says: "Availability: Unix.", so OSError is expected in some cases
    src_path = os.path.join(keyrings_base_dir, "../wallet")
    link_path = os.path.join(keyrings_base_dir, "wallet")
    try:
        os.symlink(src_path, link_path)
    except OSError:
        logger.warning('Failed to create symlink {} for {}'.format(
            link_path, src_path), exc_info=True)
    else:
        inv_paths.append('wallet')

    def check_path(path):
        with pytest.raises(ValueError) as exc_info:
            wsh.saveWallet(test_wallet, path)

        exc_info.match(
            r"path {} is not is not relative to the keyrings {}".format(
                path, keyrings_base_dir))

    for path in inv_paths:
        check_path(path)


def test_wallet_dir_path_exists_as_file(tdir_hierarchy, test_wallet):
    root, dirs, files = tdir_hierarchy

    wdir = files[0]

    wsh = WalletStorageHelper(root)
    with pytest.raises(NotADirectoryError) as exc_info:
        wsh.saveWallet(test_wallet, os.path.join(wdir, 'wallet'))

    exc_info.match(r"{}".format(wdir))


def test_new_file_wallet_permissions(
        tdir_for_func, keyrings_base_dir, test_wallet):
    wpath = 'ctx/test.wallet'

    # default
    wsh = WalletStorageHelper(keyrings_base_dir)
    wpath = '1/2/3/wallet'
    wpath_res = wsh.saveWallet(test_wallet, wpath)
    check_permissions(wpath_res, DEFAULT_FMODE)

    # non-default
    fmode = DEFAULT_DMODE + 1
    wsh = WalletStorageHelper(keyrings_base_dir, fmode=fmode)
    wpath = '4/5/6/wallet'
    wpath_res = wsh.saveWallet(test_wallet, wpath)
    check_permissions(wpath_res, fmode)


def test_existed_wallet_permissions(tdir_hierarchy, test_wallet):
    root, dirs, files = tdir_hierarchy
    wpath = os.path.join(root, files[0])
    mode1 = get_permissions(wpath)
    mode2 = mode1 | NOT_LISTED_PERMISSION
    wsh = WalletStorageHelper(root, fmode=mode2)
    wsh.saveWallet(test_wallet, files[0])
    check_permissions(wpath, mode2)


def test_store_wallet_by_abs_path(
        tdir_for_func, keyrings_base_dir, test_wallet):
    wsh = WalletStorageHelper(keyrings_base_dir)
    abs_path = os.path.join(keyrings_base_dir, "1/2/3/wallet")
    wsh.saveWallet(test_wallet, abs_path)
    check_permissions(abs_path, DEFAULT_FMODE)


def test_stored_wallet_data(tdir_for_func, keyrings_base_dir, test_wallet):
    wpath = 'ctx/test.wallet'

    wsh = WalletStorageHelper(keyrings_base_dir)

    wpath_res = wsh.saveWallet(test_wallet, wpath)
    assert wpath_res == os.path.join(keyrings_base_dir, wpath)

    with open(wpath_res) as wf:
        wdata = wf.read()

    # TODO no comparison operator for Wallet
    assert wdata == encode_wallet(test_wallet)


def test_load_wallet_by_empty_path_fail(tdir_for_func, keyrings_base_dir):
    wsh = WalletStorageHelper(keyrings_base_dir)

    for path in (None, ''):
        with pytest.raises(ValueError) as exc_info:
            wsh.loadWallet(path)

        exc_info.match(r'empty path')


def test_load_wallet_outside_fail(tdir_for_func, keyrings_base_dir):
    wsh = WalletStorageHelper(keyrings_base_dir)

    inv_paths = [
        os.path.join(keyrings_base_dir, '../wallet'),
        '../wallet',
        'a/../../wallet'
    ]

    #  docs says: "Availability: Unix.", so OSError is expected in some cases
    src_path = os.path.join(keyrings_base_dir, "../wallet")
    link_path = os.path.join(keyrings_base_dir, "wallet")
    try:
        os.symlink(src_path, link_path)
    except OSError:
        logger.warning('Failed to create symlink {} for {}'.format(
            link_path, src_path), exc_info=True)
    else:
        inv_paths.append('wallet')

    def check_path(path):
        with pytest.raises(ValueError) as exc_info:
            wsh.loadWallet(path)

        exc_info.match(
            r"path {} is not is not relative to the wallets {}".format(
                path, keyrings_base_dir))

    for path in inv_paths:
        check_path(path)


def test_loaded_wallet_data(tdir_for_func, keyrings_base_dir, test_wallet):
    wpath = 'ctx/test.wallet'

    wsh = WalletStorageHelper(keyrings_base_dir)

    wsh.saveWallet(test_wallet, wpath)
    loaded_wallet = wsh.loadWallet(wpath)
    # TODO no comparison operator for Wallet (and classes it used)
    assert encode_wallet(test_wallet) == encode_wallet(loaded_wallet)


def test_load_wallet_by_abs_path(
        tdir_for_func, keyrings_base_dir, test_wallet):
    wsh = WalletStorageHelper(keyrings_base_dir)
    abs_path = os.path.join(keyrings_base_dir, "5/6/7/wallet")
    wsh.saveWallet(test_wallet, abs_path)
    loaded_wallet = wsh.loadWallet(abs_path)
    # TODO no comparison operator for Wallet (and classes it used)
    assert encode_wallet(test_wallet) == encode_wallet(loaded_wallet)
