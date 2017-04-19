import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.test.test_file_hash_store import nodesLeaves, \
    generateHashes

from plenum.persistence.leveldb_hash_store import LevelDbHashStore


@pytest.yield_fixture(scope="module")
def leveldbHashStore(tdir):
    hs = LevelDbHashStore(tdir)
    cleanup(hs)
    yield hs
    hs.close()


def cleanup(hs):
    hs.reset()
    hs.leafCount = 0


def testIndexFrom1(leveldbHashStore):
    with pytest.raises(IndexError):
        leveldbHashStore.readLeaf(0)


def testReadWrite(leveldbHashStore, nodesLeaves):
    nodes, leaves = nodesLeaves
    for node in nodes:
        leveldbHashStore.writeNode(node)
    for leaf in leaves:
        leveldbHashStore.writeLeaf(leaf)
    onebyone = [leveldbHashStore.readLeaf(i + 1) for i in range(10)]
    multiple = leveldbHashStore.readLeafs(1, 10)
    assert onebyone == leaves
    assert onebyone == multiple


def testRecoverLedgerFromHashStore(leveldbHashStore, tdir):
    cleanup(leveldbHashStore)
    tree = CompactMerkleTree(hashStore=leveldbHashStore)
    ledger = Ledger(tree=tree, dataDir=tdir)
    for d in range(10):
        ledger.add(str(d).encode())
    updatedTree = ledger.tree
    ledger.stop()

    tree = CompactMerkleTree(hashStore=leveldbHashStore)
    restartedLedger = Ledger(tree=tree, dataDir=tdir)
    assert restartedLedger.size == ledger.size
    assert restartedLedger.root_hash == ledger.root_hash
    assert restartedLedger.tree.hashes == updatedTree.hashes
    assert restartedLedger.tree.root_hash == updatedTree.root_hash
    restartedLedger.stop()
