import pyorient
import pytest

from plenum.persistence.orientdb_store import OrientDbStore
from plenum.persistence.orientdb_hash_store import OrientDbHashStore
from ledger.immutable_store.test.test_file_hash_store import nodesLeaves, \
    generateHashes


@pytest.fixture(scope="module")
def odbhs():
    hs = OrientDbHashStore(
        OrientDbStore(user="root", password="password", dbName="test"))
    truncateClasses(hs)
    return hs


def truncateClasses(hs):
    for cls in [hs.nodeHashClass, hs.leafHashClass]:
        if hs.store.classExists(cls):
            hs.store.client.command("Truncate class {}".format(cls))


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
    nodes, leafs = nodesLeaves
    for node in nodes:
        odbhs.writeNode(node)
    for leaf in leafs:
        odbhs.writeLeaf(leaf)
    onebyone = [odbhs.readLeaf(i + 1) for i in range(10)]
    multiple = odbhs.readLeafs(1, 10)
    assert onebyone == multiple


def testUniqueConstraint(odbhs: OrientDbHashStore):
    leafHash = generateHashes(1)[0]
    odbhs.writeLeaf(leafHash)
    with pytest.raises(pyorient.PyOrientORecordDuplicatedException):
        odbhs.writeLeaf(leafHash)
