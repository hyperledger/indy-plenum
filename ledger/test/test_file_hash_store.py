import string
from hashlib import sha256
from random import choice, randint

import pytest

from ledger.stores.file_hash_store import FileHashStore


@pytest.fixture(scope="module")
def nodesLeaves():
    return [(randint(0, 1000000), randint(0, 255), h)
            for h in generateHashes(10)], generateHashes(10)


def writtenFhs(tempdir, nodes, leaves):
    fhs = FileHashStore(tempdir)
    for leaf in leaves:
        fhs.writeLeaf(leaf)
    for node in nodes:
        fhs.writeNode(node)
    return fhs


def generateHashes(count=10):
    return [sha256(
        (choice(string.ascii_letters) * (randint(i, 1000) if i < 1000
                                         else randint(1000, i))).encode()
    ).digest() for i in range(count)]


def testSimpleReadWrite(nodesLeaves, tempdir):
    nodes, leaves = nodesLeaves
    fhs = FileHashStore(tempdir)

    for leaf in leaves:
        fhs.writeLeaf(leaf)
    for i, leaf in enumerate(leaves):
        assert leaf == fhs.readLeaf(i + 1)

    for node in nodes:
        fhs.writeNode(node)
    for i, node in enumerate(nodes):
        assert node[2] == fhs.readNode(i + 1)

    lvs = fhs.readLeafs(1, len(leaves))
    for i, l in enumerate(lvs):
        assert leaves[i] == l

    nds = fhs.readNodes(1, len(nodes))
    for i, n in enumerate(nds):
        assert nodes[i][2] == n


def testIncorrectWrites(tempdir):
    fhs = FileHashStore(tempdir, leafSize=50, nodeSize=50)

    with pytest.raises(ValueError):
        fhs.writeLeaf(b"less than 50")
    with pytest.raises(ValueError):
        fhs.writeNode((8, 1, b"also less than 50"))

    with pytest.raises(ValueError):
        fhs.writeLeaf(b"more than 50" + b'1'*50)
    with pytest.raises(ValueError):
        fhs.writeNode((4, 1, b"also more than 50" + b'1'*50))


def testRandomAndRepeatedReads(nodesLeaves, tempdir):
    nodes, leaves = nodesLeaves
    fhs = writtenFhs(tempdir=tempdir, nodes=nodes, leaves=leaves)

    for i in range(10):
        idx = choice(range(len(leaves)))
        assert leaves[idx] == fhs.readLeaf(idx + 1)

    for i in range(10):
        idx = choice(range(len(nodes)))
        assert nodes[idx][2] == fhs.readNode(idx + 1)

    idx = len(leaves) // 2
    # Even if same leaf is read more than once it should return the
    # same value. It checks for proper uses of `seek` method
    assert leaves[idx] == fhs.readLeaf(idx + 1)
    assert leaves[idx] == fhs.readLeaf(idx + 1)

    # Even after writing some data, the data at a previous index should not
    # change
    fhs.writeLeaf(leaves[-1])
    fhs.writeLeaf(leaves[0])
    assert leaves[idx] == fhs.readLeaf(idx + 1)
