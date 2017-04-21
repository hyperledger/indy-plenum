import pyorient
import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.test.test_file_hash_store import nodesLeaves, \
    generateHashes

from plenum.persistence.orientdb_hash_store import OrientDbHashStore
from plenum.persistence.orientdb_store import OrientDbStore


@pytest.fixture(scope="module")
def odbhs():
    hs = OrientDbHashStore(
        OrientDbStore(user="root", password="password", dbName="test"))
    cleanup(hs)
    return hs


def cleanup(hs):
    for cls in [hs.nodeHashClass, hs.leafHashClass]:
        if hs.store.classExists(cls):
            hs.store.client.command("Truncate class {}".format(cls))
    hs.leafCount = 0


def testOrientDbSetup(odbhs):
    store = odbhs.store
    # This seems to be a bug in pyorient. Reported. Bug #186
    # assert store.client.db_exists("test", pyorient.STORAGE_TYPE_MEMORY)
    assert store.classExists(odbhs.leafHashClass)
    assert store.classExists(odbhs.nodeHashClass)


def testIndexFrom1(odbhs: OrientDbHashStore):
    with pytest.raises(IndexError):
        odbhs.readLeaf(0)


def testReadWrite(odbhs: OrientDbHashStore, nodesLeaves):
    nodes, leaves = nodesLeaves
    for node in nodes:
        odbhs.writeNode(node)
    for leaf in leaves:
        odbhs.writeLeaf(leaf)
    onebyone = [odbhs.readLeaf(i + 1) for i in range(10)]
    multiple = odbhs.readLeafs(1, 10)
    assert onebyone == leaves
    assert onebyone == multiple


def testUniqueConstraint(odbhs: OrientDbHashStore):
    leafHash = generateHashes(1)[0]
    odbhs.writeLeaf(leafHash)
    with pytest.raises(pyorient.PyOrientORecordDuplicatedException):
        odbhs.writeLeaf(leafHash)


def testRecoverLedgerFromHashStore(odbhs, tdir):
    cleanup(odbhs)
    tree = CompactMerkleTree(hashStore=odbhs)
    ledger = Ledger(tree=tree, dataDir=tdir)
    for d in range(10):
        ledger.add(str(d).encode())
    updatedTree = ledger.tree
    ledger.stop()

    tree = CompactMerkleTree(hashStore=odbhs)
    restartedLedger = Ledger(tree=tree, dataDir=tdir)
    assert restartedLedger.size == ledger.size
    assert restartedLedger.root_hash == ledger.root_hash
    assert restartedLedger.tree.hashes == updatedTree.hashes
    assert restartedLedger.tree.root_hash == updatedTree.root_hash
    restartedLedger.stop()
