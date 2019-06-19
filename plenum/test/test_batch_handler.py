import pytest
import time

from plenum.server.batch_handlers.ts_store_batch_handler import TsStoreBatchHandler
from storage.state_ts_store import StateTsDbStorage

from plenum.common.constants import TS_LABEL
from plenum.server.batch_handlers.domain_batch_handler import DomainBatchHandler
from plenum.server.database_manager import DatabaseManager
from storage.kv_store_rocksdb import KeyValueStorageRocksdb
from state.pruning_state import PruningState
from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.ledger import Ledger
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch

LEDGER_ID = 1
FIXED_HASH = 'CMEcrTyug8SYVkLtR8qgjqx7NVEQ4xXuAvaJDDPhDu8n'
FIXED_DOMAIN_HASH = '6gAvRxLSHhWuedwKXuoENg6WXVeYzcyDeQRYnFQmrtbn'


@pytest.fixture(scope='function')
def database_manager(tdir_for_func):
    db = DatabaseManager()
    db.register_new_database(LEDGER_ID, Ledger(CompactMerkleTree(), dataDir=tdir_for_func), PruningState(
        KeyValueStorageRocksdb(tdir_for_func, 'kv1')))

    db.register_new_store(TS_LABEL, StateTsDbStorage('test', {1: KeyValueStorageRocksdb(tdir_for_func, 'kv2')}))
    return db


@pytest.fixture(scope='function')
def batch_handler(database_manager):
    bh = BatchRequestHandler(database_manager, LEDGER_ID)
    bh.ledger.appendTxns([{'txnMetadata': {'seqNo': 1}}])
    return bh


@pytest.fixture(scope='function')
def domain_batch_handler(database_manager):
    bh = DomainBatchHandler(database_manager)
    bh.ledger.appendTxns([{'txnMetadata': {'seqNo': 1}}])
    return bh


@pytest.fixture(scope='function')
def ts_store_batch_handler(database_manager):
    return TsStoreBatchHandler(database_manager)


@pytest.fixture(scope='function')
def three_pc_batch(batch_handler):
    # Constant root hash is one which will be formed after applying txn
    return ThreePcBatch(LEDGER_ID, 0, 0, 1, time.time(),
                        batch_handler.state.headHash,
                        FIXED_HASH,
                        ['a', 'b', 'c'], ['d1', 'd2', 'd3'])


def test_batch_handler_commit(batch_handler: BatchRequestHandler, three_pc_batch):
    assert len(batch_handler.ledger) == 0

    batch_handler.commit_batch(three_pc_batch)

    assert len(batch_handler.ledger) == 1

    batch_handler._check_consistency_after_commit(FIXED_HASH)


def test_ts_store_batch_handler(ts_store_batch_handler, three_pc_batch):
    three_pc_batch.txn_root = FIXED_DOMAIN_HASH
    ts_store_batch_handler.commit_batch(three_pc_batch)
    assert ts_store_batch_handler.database_manager.ts_store.get(three_pc_batch.pp_time) == three_pc_batch.state_root
