import os
from typing import Any

import pytest
from common.serializers.json_serializer import JsonSerializer
from crypto.bls.bls_crypto import BlsSerializer
from plenum.bls.bls_key_manager_file import BlsKeyManagerFile
from stp_zmq.test.helper import get_file_permission_mask


@pytest.fixture()
def tempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath


@pytest.fixture()
def bls_json_serializer():
    class JsonBlsSerializer(BlsSerializer):
        def __init__(self):
            super().__init__(None)
            self.json_ser = JsonSerializer()

        def serialize_to_bytes(self, obj: Any) -> bytes:
            return self.json_ser.serialize(obj, toBytes=True)

        def deserialize_from_bytes(self, obj: bytes) -> Any:
            return self.json_ser.deserialize(obj)

        def serialize_to_str(self, obj: Any) -> str:
            return self.json_ser.serialize(obj, toBytes=False)

        def deserialize_from_str(self, obj: str) -> Any:
            return self.json_ser.deserialize(obj)

    return JsonBlsSerializer()


@pytest.fixture()
def bls_key_manager_file(tempdir, bls_json_serializer):
    os.mkdir(os.path.join(tempdir, 'Node1'))
    return BlsKeyManagerFile(bls_json_serializer, tempdir, 'Node1')


def test_key_dir(bls_key_manager_file):
    assert bls_key_manager_file._bls_keys_dir
    assert os.path.isdir(bls_key_manager_file._bls_keys_dir)
    assert '744' == get_file_permission_mask(bls_key_manager_file._bls_keys_dir)


def test_save_keys(bls_key_manager_file):
    sk = {'aaa': 'bbb', 'ccc': 'ddd'}
    pk = {'333': '222', '111': '000'}
    saved_sk, saved_pk = bls_key_manager_file.save_keys(sk, pk)
    assert isinstance(saved_sk, bytes)
    assert isinstance(saved_pk, bytes)


def test_load_keys(bls_key_manager_file):
    sk = {'aaa': 'bbb', 'ccc': 'ddd'}
    pk = {'333': '222', '111': '000'}
    bls_key_manager_file.save_keys(sk, pk)
    loaded_sk, loaded_pk = bls_key_manager_file.load_keys()
    assert sk == loaded_sk
    assert pk == loaded_pk


def test_files_permissions(bls_key_manager_file):
    sk = {'aaa': 'bbb', 'ccc': 'ddd'}
    pk = {'333': '222', '111': '000'}
    bls_key_manager_file.save_keys(sk, pk)
    sk_file = os.path.join(bls_key_manager_file._bls_keys_dir, bls_key_manager_file.BLS_SK_FILE_NAME)
    pk_file = os.path.join(bls_key_manager_file._bls_keys_dir, bls_key_manager_file.BLS_PK_FILE_NAME)
    assert '600' == get_file_permission_mask(sk_file)
    assert '644' == get_file_permission_mask(pk_file)
