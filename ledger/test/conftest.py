from collections import OrderedDict

import pytest
from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
from ledger.serializers.compact_serializer import CompactSerializer
from ledger.serializers.json_serializer import JsonSerializer
from ledger.serializers.msgpack_serializer import MsgPackSerializer
from ledger.test.helper import create_ledger


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
    initiator = GenesisTxnInitiatorFromFile(tempdir, "init_file")
    ledger = initiator.create_initiator_ledger()
    for txn in genesis_txns:
        ledger.add(txn)
    return "init_file"

@pytest.fixture(scope="function")
def init_genesis_txn_file(request, tempdir, genesis_txns):
    initiator = GenesisTxnInitiatorFromFile(tempdir, "init_file")
    ledger = initiator.create_initiator_ledger()
    for txn in genesis_txns:
        ledger.add(txn)
    return "init_file"

@pytest.yield_fixture(scope="function", params=['MsgPack', 'Json', 'Compact'])
def serializer(request):
    if request.param == 'MsgPack':
        return MsgPackSerializer()
    if request.param == 'Json':
        return JsonSerializer()
    if request.param == 'Compact':
        return CompactSerializer(orderedFields)


@pytest.yield_fixture(scope="function", params=['TextFileStorage', 'ChunkedFileStorage', 'LeveldbStorage'])
def ledger(request, genesis_txn_file, tempdir, serializer):
    return create_ledger(request, serializer, tempdir, genesis_txn_file)


@pytest.yield_fixture(scope="function", params=['TextFileStorage', 'ChunkedFileStorage', 'LeveldbStorage'])
def ledger_no_genesis(request, tempdir, serializer):
    return create_ledger(request, serializer, tempdir)

@pytest.yield_fixture(scope="function", params=['TextFileStorage', 'ChunkedFileStorage', 'LeveldbStorage'])
def ledger_with_genesis(request, init_genesis_txn_file, tempdir, serializer):
    return create_ledger(request, serializer, tempdir, init_genesis_txn_file)
