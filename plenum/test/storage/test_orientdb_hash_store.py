import pyorient
import pytest

from persistence.orientdb_hash_store import OrientDbHashStore
from ledger.immutable_store.test.test_file_hash_store import nodesLeaves


@pytest.fixture(scope="module")
def db():
    # TODO Read setup info from a config file
    return OrientDbHashStore(user="root", password="password", dbName="test")

"""
Tests:

1. Check the connection and that the two stores are present in the db.
2. Write nodes and leaves and retrieve them one at a time.
3. Write nodes and leaves and retrieve them in bulk.
4. testRandomAndRepeatedReads (use some code form test_file_hash_store.py
"""


def testOrientDbSetup():
    db = OrientDbHashStore(user="root", password="password", dbName="test")
    assert db.client.db_exists("test", pyorient.STORAGE_TYPE_MEMORY)
    assert db.classExists(db.leafHashClass)
    assert db.classExists(db.nodeHashClass)


def testOneByOne(db: OrientDbHashStore, nodesLeaves):
    nodes, leafs = nodesLeaves
    for node in nodes:
        db.writeNode(node)
    for leaf in leafs:
        db.writeLeaf(leaf)



