import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.test.test_file_hash_store import nodesLeaves
from plenum.persistence.leveldb_hash_store import LevelDbHashStore
from plenum.persistence.rocksdb_hash_store import RocksDbHashStore


@pytest.yield_fixture(scope="module", params=['rocksdb', 'leveldb'])
def hashStore(request, tmpdir_factory):
    if request.param == 'rocksdb':
        hs = RocksDbHashStore(tmpdir_factory.mktemp('').strpath)
    else:
        hs = LevelDbHashStore(tmpdir_factory.mktemp('').strpath)

    cleanup(hs)
    yield hs
    hs.close()


def cleanup(hs):
    hs.reset()
    hs.leafCount = 0


def testIndexFrom1(hashStore):
    with pytest.raises(IndexError):
        hashStore.readLeaf(0)


def testReadWrite(hashStore, nodesLeaves):
    nodes, leaves = nodesLeaves
    for node in nodes:
        hashStore.writeNode(node)
    for leaf in leaves:
        hashStore.writeLeaf(leaf)
    onebyone = [hashStore.readLeaf(i + 1) for i in range(10)]
    multiple = hashStore.readLeafs(1, 10)
    assert onebyone == leaves
    assert onebyone == multiple


def testRecoverLedgerFromHashStore(hashStore, tdir):
    cleanup(hashStore)
    tree = CompactMerkleTree(hashStore=hashStore)
    ledger = Ledger(tree=tree, dataDir=tdir)
    for d in range(10):
        ledger.add(str(d).encode())
    updatedTree = ledger.tree
    ledger.stop()

    tree = CompactMerkleTree(hashStore=hashStore)
    restartedLedger = Ledger(tree=tree, dataDir=tdir)
    assert restartedLedger.size == ledger.size
    assert restartedLedger.root_hash == ledger.root_hash
    assert restartedLedger.tree.hashes == updatedTree.hashes
    assert restartedLedger.tree.root_hash == updatedTree.root_hash
    restartedLedger.stop()
