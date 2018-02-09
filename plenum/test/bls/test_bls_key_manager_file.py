import json
import os

import pytest
from plenum.bls.bls_key_manager_file import BlsKeyManagerFile
from stp_zmq.test.helper import get_file_permission_mask


@pytest.fixture()
def bls_key_manager_file(tdir_for_func):
    dir = os.path.join(tdir_for_func, 'Node1')
    os.mkdir(dir)
    return BlsKeyManagerFile(dir)


def test_key_dir(bls_key_manager_file):
    assert bls_key_manager_file._bls_keys_dir
    assert os.path.isdir(bls_key_manager_file._bls_keys_dir)
    assert '744' == get_file_permission_mask(bls_key_manager_file._bls_keys_dir)


def test_save_keys(bls_key_manager_file):
    sk = json.dumps(
        {'aaa': 'bbb', 'ccc': 'ddd'})
    pk = json.dumps(
        {'333': '222', '111': '000'})
    saved_sk, saved_pk = bls_key_manager_file.save_keys(sk, pk)
    assert isinstance(saved_sk, str)
    assert isinstance(saved_pk, str)


def test_save_keys_not_str(bls_key_manager_file):
    sk = {'aaa': 'bbb', 'ccc': 'ddd'}
    pk = {'333': '222', '111': '000'}
    with pytest.raises(AssertionError):
        bls_key_manager_file.save_keys(sk, pk)


def test_load_keys(bls_key_manager_file):
    sk = json.dumps(
        {'aaa': 'bbb', 'ccc': 'ddd'})
    pk = json.dumps(
        {'333': '222', '111': '000'})
    bls_key_manager_file.save_keys(sk, pk)
    loaded_sk, loaded_pk = bls_key_manager_file.load_keys()
    assert sk == loaded_sk
    assert pk == loaded_pk


def test_files_permissions(bls_key_manager_file):
    sk = json.dumps(
        {'aaa': 'bbb', 'ccc': 'ddd'})
    pk = json.dumps(
        {'333': '222', '111': '000'})
    bls_key_manager_file.save_keys(sk, pk)
    sk_file = os.path.join(bls_key_manager_file._bls_keys_dir, bls_key_manager_file.BLS_SK_FILE_NAME)
    pk_file = os.path.join(bls_key_manager_file._bls_keys_dir, bls_key_manager_file.BLS_PK_FILE_NAME)
    assert '600' == get_file_permission_mask(sk_file)
    assert '644' == get_file_permission_mask(pk_file)
