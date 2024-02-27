from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.ledger import Ledger
from plenum.persistence.db_hash_store import DbHashStore
from state.pruning_state import PruningState
from storage.helper import initKeyValueStorage
from plenum.common.constants import HS_LEVELDB


def get_did_plugin_hash_store(data_dir):
    return DbHashStore(dataDir=data_dir,
                       fileNamePrefix='did_plugin',
                       db_type=HS_LEVELDB)


def get_did_plugin_ledger(data_dir, name, hash_store, config):
    return Ledger(CompactMerkleTree(hashStore=hash_store),
                  dataDir=data_dir,
                  fileName=name,
                  ensureDurability=config.EnsureLedgerDurability)


def get_did_plugin_state(data_dir, name, config):
    return PruningState(initKeyValueStorage(
        config.didPluginStateStorage, data_dir, name))
