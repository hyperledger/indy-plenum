import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.test.test_file_hash_store import nodesLeaves, \
    generateHashes

from plenum.persistence.rocksdb_hash_store import RocksDbHashStore


@pytest.yield_fixture(scope="module")
def rocksdbHashStore(tdir):
    hs = RocksDbHashStore(tdir)
    cleanup(hs)
    yield hs
    hs.close()


def cleanup(hs):
    hs.reset()
    hs.leafCount = 0


def testIndexFrom1(rocksdbHashStore):
    with pytest.raises(IndexError):
        rocksdbHashStore.readLeaf(0)


def testReadWrite(rocksdbHashStore, nodesLeaves):
    nodes, leaves = nodesLeaves
    for node in nodes:
        rocksdbHashStore.writeNode(node)
    for leaf in leaves:
        rocksdbHashStore.writeLeaf(leaf)
    onebyone = [rocksdbHashStore.readLeaf(i + 1) for i in range(10)]
    multiple = rocksdbHashStore.readLeafs(1, 10)
    assert onebyone == leaves
    assert onebyone == multiple


def testRecoverLedgerFromHashStore(rocksdbHashStore, tdir):
    cleanup(rocksdbHashStore)
    tree = CompactMerkleTree(hashStore=rocksdbHashStore)
    ledger = Ledger(tree=tree, dataDir=tdir)
    for d in range(10):
        ledger.add(str(d).encode())
    updatedTree = ledger.tree
    ledger.stop()

    tree = CompactMerkleTree(hashStore=rocksdbHashStore)
    restartedLedger = Ledger(tree=tree, dataDir=tdir)
    assert restartedLedger.size == ledger.size
    assert restartedLedger.root_hash == ledger.root_hash
    assert restartedLedger.tree.hashes == updatedTree.hashes
    assert restartedLedger.tree.root_hash == updatedTree.root_hash
    restartedLedger.stop()
