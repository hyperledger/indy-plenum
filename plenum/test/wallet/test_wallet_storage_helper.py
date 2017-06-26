import pytest
import os
import stat
import jsonpickle

from plenum.client.wallet import WalletStorageHelper as WSH

DEFAULT_DMODE = 0o700
DEFAULT_FMODE = 0o600


def encode_wallet(wallet):
    return jsonpickle.encode(wallet, keys=True)


def decode_wallet(wdata):
    return jsonpickle.decode(wdata, keys=True)


@pytest.fixture(scope='function')
def tdir_hierarchy(tdir_for_func):
    tdir_for_func = os.path.join(tdir_for_func, 'root')
    dirs = ['1/2/3', 'a/b/c']
    files = ['1/2/3/1.txt', 'a/2.txt', 'a/b/3.txt']
    for d in dirs:
        os.makedirs(os.path.join(tdir_for_func, d))
    for f in files:
        open(os.path.join(tdir_for_func, f), 'a').close()
    return (tdir_for_func, dirs, files)


@pytest.fixture(scope='function')
def keyrings_base_dir(tdir_for_func):
    return os.path.join(tdir_for_func, 'keyrings')


@pytest.fixture(scope='function')
def test_wallet():
    return {1: 2, 3: {5: 6}}  # TODO real wallet


def get_permissions(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def check_permissions(path, mode):
    assert get_permissions(path) == mode


def test_keyring_base_dir_new_permissions(tdir_for_func):
    # default
    keyringsBaseDir = os.path.join(tdir_for_func, 'keyrings')
    WSH(keyringsBaseDir)
    check_permissions(keyringsBaseDir, DEFAULT_DMODE)

    # non-default
    dmode = DEFAULT_DMODE + 1
    keyringsBaseDir = os.path.join(tdir_for_func, 'keyrings2')
    WSH(keyringsBaseDir, dmode=dmode)
    check_permissions(keyringsBaseDir, dmode)


def test_keyring_base_dir_exists_as_file(tdir_hierarchy):
    root, dirs, files = tdir_hierarchy
    with pytest.raises(NotADirectoryError):
        WSH(os.path.join(root, files[0]))


def test_keyring_base_dir_exists_as_dir(tdir_hierarchy):
    root, dirs, files = tdir_hierarchy
    dpath = os.path.join(root, dirs[0])
    mode1 = get_permissions(dpath)
    mode2 = mode1 + 1
    WSH(dpath)
    check_permissions(dpath, mode2)


def test_store_wallet_by_empty_path_fail(tdir_for_func, keyrings_base_dir, test_wallet):

    wsh = WSH(keyrings_base_dir)

    for path in (None, ''):
        with pytest.raises(TypeError) as exc_info:
            wsh.saveWallet(test_wallet, path)

        exc_info.match(r'empty path')


def test_store_wallet_by_abs_path_fail(tdir_for_func, keyrings_base_dir, test_wallet):

    wsh = WSH(keyrings_base_dir)
    abs_path = "/1/2/3/wallet"

    with pytest.raises(TypeError) as exc_info:
        wsh.saveWallet(test_wallet, abs_path)

    exc_info.match(r'path {} is absolute'.format(abs_path))


def test_store_wallet_outside_fail(tdir_for_func, keyrings_base_dir, test_wallet):

    wsh = WSH(keyrings_base_dir)

    inv_paths = ['../wallet', 'a/../../wallet', 'b/./../wallet']
    for path in inv_paths:
        with pytest.raises(TypeError) as exc_info:
            wsh.saveWallet(test_wallet, path)

        exc_info.match(r"path {} is not insdide the keyrings {}".format(
            path, keyrings_base_dir))


def test_wallet_dir_path_exists_as_file(tdir_hierarchy, test_wallet):
    root, dirs, files = tdir_hierarchy

    wdir = files[0]

    wsh = WSH(root)
    with pytest.raises(NotADirectoryError) as exc_info:
        wsh.saveWallet(test_wallet, os.path.join(wdir, 'wallet'))

        exc_info.match(r"{}".format(wdir))



def test_new_file_wallet_permissions(tdir_for_func, keyrings_base_dir, test_wallet):
    wpath = 'ctx/test.wallet'

    # default
    wsh = WSH(keyrings_base_dir)
    wpath = '1/2/3/wallet'
    wpath_res = wsh.saveWallet(test_wallet, wpath)
    check_permissions(wpath_res, DEFAULT_FMODE)

    # non-default
    fmode = DEFAULT_DMODE + 1
    wsh = WSH(keyrings_base_dir, fmode=fmode)
    wpath = '4/5/6/wallet'
    wpath_res = wsh.saveWallet(test_wallet, wpath)
    check_permissions(wpath_res, fmode)


def test_existed_wallet_permissions(tdir_hierarchy, test_wallet):
    root, dirs, files = tdir_hierarchy
    wpath = os.path.join(root, files[0])
    mode1 = get_permissions(wpath)
    mode2 = mode1 + 1
    wsh = WSH(root, fmode=mode2)
    wsh.saveWallet(test_wallet, wpath)
    check_permissions(wpath, mode2)


def test_stored_wallet_data(tdir_for_func, keyrings_base_dir, test_wallet):
    wpath = 'ctx/test.wallet'

    wsh = WSH(keyrings_base_dir)

    wpath_res = wsh.saveWallet(test_wallet, wpath)
    assert wpath_res == os.path.join(keyrings_base_dir, wpath)

    with open(wpath_res, "rb") as wf:
        wdata = wf.read()

    assert wdata == encode_wallet(test_wallet).encode()
    assert test_wallet == decode_wallet(wdata.decode())

