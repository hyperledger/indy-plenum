from collections import OrderedDict

import pytest
from common.serializers.compact_serializer import CompactSerializer
from common.serializers.json_serializer import JsonSerializer
from common.serializers.msgpack_serializer import MsgPackSerializer
from common.serializers.signing_serializer import SigningSerializer
from ledger.genesis_txn.genesis_txn_file_util import create_genesis_txn_init_ledger
from ledger.test.helper import create_ledger, create_ledger_text_file_storage, \
    create_ledger_chunked_file_storage, create_ledger_leveldb_storage, create_ledger_rocksdb_storage


@pytest.fixture(scope='module')
def tdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath


@pytest.fixture(scope='function')
def tempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath


orderedFields = OrderedDict([
    ("identifier", (str, str)),
    ("reqId", (str, int)),
    ("op", (str, str))
])


@pytest.fixture(scope="function")
def genesis_txns():
    return [{"reqId": 1, "identifier": "5rArie7XKukPCaEwq5XGQJnM9Fc5aZE3M9HAPVfMU2xC", "op": "op1"},
            {"reqId": 2, "identifier": "2btLJAAb1S3x6hZYdVyAePjqtQYi2ZBSRGy4569RZu8h", "op": "op2"},
            {"reqId": 3, "identifier": "CECeGXDi6EHuhpwz19uyjjEnsRGNXodFYqCRgdLmLRkt", "op": "op3"}
            ]


@pytest.yield_fixture(scope="function", params=['with_genesis', 'without_genesis'])
def genesis_txn_file(request, tempdir, genesis_txns):
    if request.param == 'without_genesis':
        return None
    ledger = create_genesis_txn_init_ledger(tempdir, "init_file")
    for txn in genesis_txns:
        ledger.add(txn)
    return "init_file"


@pytest.fixture(scope="function")
def init_genesis_txn_file(request, tempdir, genesis_txns):
    ledger = create_genesis_txn_init_ledger(tempdir, "init_file")
    for txn in genesis_txns:
        ledger.add(txn)
    return "init_file"


@pytest.yield_fixture(scope="function", params=['MsgPack', 'Json', 'Compact'])
def txn_serializer(request):
    if request.param == 'MsgPack':
        return MsgPackSerializer()
    if request.param == 'Json':
        return JsonSerializer()
    if request.param == 'Compact':
        return CompactSerializer(orderedFields)


@pytest.yield_fixture(scope="function", params=['MsgPack', 'Json', 'Compact', 'Signing'])
def hash_serializer(request):
    if request.param == 'MsgPack':
        return MsgPackSerializer()
    if request.param == 'Json':
        return JsonSerializer()
    if request.param == 'Signing':
        return SigningSerializer()
    if request.param == 'Compact':
        return CompactSerializer(orderedFields)


@pytest.yield_fixture(scope="function", params=['TextFileStorage', 'ChunkedFileStorage',
                                                'LeveldbStorage', 'RocksdbStorage'])
def ledger(request, genesis_txn_file, tempdir, txn_serializer, hash_serializer):
    ledger = create_ledger(request, txn_serializer,
                           hash_serializer, tempdir, genesis_txn_file)
    yield ledger
    ledger.stop()


@pytest.yield_fixture(scope="function", params=['TextFileStorage', 'ChunkedFileStorage',
                                                'LeveldbStorage', 'RocksdbStorage'])
def create_ledger_callable(request):
    if request.param == 'TextFileStorage':
        return create_ledger_text_file_storage
    elif request.param == 'ChunkedFileStorage':
        return create_ledger_chunked_file_storage
    elif request.param == 'LeveldbStorage':
        return create_ledger_leveldb_storage
    elif request.param == 'RocksdbStorage':
        return create_ledger_rocksdb_storage


@pytest.yield_fixture(scope="function", params=['TextFileStorage', 'ChunkedFileStorage',
                                                'LeveldbStorage', 'RocksdbStorage'])
def ledger_no_genesis(request, tempdir, txn_serializer, hash_serializer):
    ledger = create_ledger(request, txn_serializer, hash_serializer, tempdir)
    yield ledger
    ledger.stop()


@pytest.yield_fixture(scope="function", params=['TextFileStorage', 'ChunkedFileStorage',
                                                'LeveldbStorage', 'RocksdbStorage'])
def ledger_with_genesis(request, init_genesis_txn_file, tempdir, txn_serializer, hash_serializer):
    ledger = create_ledger(request, txn_serializer,
                           hash_serializer, tempdir, init_genesis_txn_file)
    yield ledger
    ledger.stop()
