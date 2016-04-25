import pyorient
import pytest

from plenum.persistence.orientdb_store import OrientDbStore
from plenum.persistence.orientdb_hash_store import OrientDbHashStore
from ledger.immutable_store.test.test_file_hash_store import nodesLeaves


@pytest.fixture(scope="module")
def db():
    # TODO Read setup info from a config file
    db = OrientDbHashStore(
        OrientDbStore(user="root", password="password", dbName="test"))
    for cls in [db.nodeHashClass, db.leafHashClass]:
        if db.store.classExists(cls):
            db.store.client.command("Truncate class {}".format(cls))
    return db


"""
Tests:

1. Check the connection and that the two stores are present in the db.
2. Write nodes and leaves and retrieve them one at a time.
3. Write nodes and leaves and retrieve them in bulk.
4. testRandomAndRepeatedReads (use some code form test_file_hash_store.py
"""


def testOrientDbSetup(db):
    store = db.store
    # This seems to be a bug in pyorient. Reported. Bug #186
    # assert store.client.db_exists("test", pyorient.STORAGE_TYPE_MEMORY)
    assert store.classExists(db.leafHashClass)
    assert store.classExists(db.nodeHashClass)


def testUniqueConstraint(db: OrientDbHashStore, nodesLeaves):
    nodes, leafs = nodesLeaves
    with pytest.raises(Exception):
        db.writeLeaf(leafs[0])
        db.writeLeaf(leafs[0])


def testIndexFrom1(db: OrientDbHashStore):
    with pytest.raises(IndexError):
        db.readLeaf(0)


def testReadWrite(db: OrientDbHashStore, nodesLeaves):
    nodes, leafs = nodesLeaves
    for node in nodes:
        db.writeNode(node)
    for leaf in leafs:
        db.writeLeaf(leaf)
    assert db.readLeaf(1)
    # TODO Read multiple and read one at a time give the same result.
