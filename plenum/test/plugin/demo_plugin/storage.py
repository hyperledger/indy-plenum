from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.ledger import Ledger
from plenum.persistence.leveldb_hash_store import LevelDbHashStore
from plenum.persistence.storage import initKeyValueStorage
from state.pruning_state import PruningState


def get_auction_hash_store(data_dir):
    return LevelDbHashStore(dataDir=data_dir,
                            fileNamePrefix='auction')


def get_auction_ledger(data_dir, name, hash_store, config):
    return Ledger(CompactMerkleTree(hashStore=hash_store),
                  dataDir=data_dir,
                  fileName=name,
                  ensureDurability=config.EnsureLedgerDurability)


def get_auction_state(data_dir, name, config):
    return PruningState(initKeyValueStorage(
        config.auctionStateStorage, data_dir, name))
