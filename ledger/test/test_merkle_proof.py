import time
from binascii import hexlify, unhexlify
from copy import copy
from tempfile import TemporaryDirectory

import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.merkle_verifier import MerkleVerifier
from ledger.stores.hash_store import HashStore
from ledger.tree_hasher import TreeHasher
from ledger.stores.memory_hash_store import MemoryHashStore
from ledger.stores.file_hash_store import FileHashStore
from ledger.test.helper import checkConsistency
from ledger.util import STH

"""
1: 221

  [221]
  /
221

2: e8b

  [e8b]
  /   \
221   fa6

3: e8b, 906

      fe6
     /   \
  [e8b]  [906]
  /   \
221   fa6


4: 4c4

        [4c4]
     /         \
   e8b         9c7
  /   \       /   \
221   fa6   906   11e


5: 4c4, 533

               e10
            /       \
        [4c4]       [533]
     /         \
   e8b         9c7
  /   \       /   \
221   fa6   906   11e


6: 4c4, 2b1

                 ecc
            /          \
        [4c4]          [2b1]
     /         \       /   \
   e8b         9c7   533   3bf
  /   \       /   \
221   fa6   906   11e


7: 4c4, 2b1, 797

                    74f
            /                \
        [4c4]                429
     /         \            /    \
   e8b         9c7       [2b1]  [797]
  /   \       /   \      /   \
221   fa6   906   11e  533   3bf


8: 50f
                    [50f]
            /                \
         4c4                 fed
     /         \            /    \
   e8b         9c7        2b1     800
  /   \       /   \      /   \    /  \
221   fa6   906   11e  533   3bf 797 754


"""

"""
hexlify(c(
    c(
        c(
            l(d[0]), l(d[1])
        ),
        c(
            l(d[2]), l(d[3])
        )
    ),
    c(
        c(
            l(d[4]),l(d[5])
        ),
        l(d[6])
    )
))
"""


@pytest.yield_fixture(scope="module", params=['File', 'Memory'])
def hashStore(request, tdir):
    if request.param == 'File':
        fhs = FileHashStore(tdir)
        yield fhs
    elif request.param == 'Memory':
        yield MemoryHashStore()


@pytest.fixture()
def hasher():
    return TreeHasher()


@pytest.fixture()
def verifier(hasher):
    return MerkleVerifier(hasher=hasher)


@pytest.fixture()
def hasherAndTree(hasher):
    tdir = TemporaryDirectory().name
    store = FileHashStore(tdir)
    m = CompactMerkleTree(hasher=hasher, hashStore=store)
    return hasher, m


@pytest.fixture()
def addTxns(hasherAndTree):
    h, m = hasherAndTree

    txn_count = 1000

    auditPaths = []
    for d in range(txn_count):
        serNo = d+1
        data = str(serNo).encode()
        auditPaths.append([hexlify(h) for h in m.append(data)])

    return txn_count, auditPaths


@pytest.fixture()
def storeHashes(hasherAndTree, addTxns, hashStore):
    h, m = hasherAndTree
    mhs = m.hashStore
    return mhs


'''
14
pwr = 3
c = 8

14,8
pwr = 2
c = 4 + 8 = 12

12,2

14, 12
pwr = 1
c = 2 + 12 = 14

14,1
'''


def show(h, m, data):
    print("-" * 60)
    print("appended  : {}".format(data))
    print("hash      : {}".format(hexlify(h.hash_leaf(data))[:3]))
    print("tree size : {}".format(m.tree_size))
    print("root hash : {}".format(m.root_hash_hex[:3]))
    for i, hash in enumerate(m.hashes):
        lead = "Hashes" if i == 0 else "      "
        print("{}    : {}".format(lead, hexlify(hash)[:3]))


def testCompactMerkleTree2(hasherAndTree, verifier):
    h, m = hasherAndTree
    v = verifier
    for serNo in range(1, 4):
        data = hexlify(str(serNo).encode())
        m.append(data)


def testCompactMerkleTree(hasherAndTree, verifier):
    h, m = hasherAndTree
    printEvery = 1000
    count = 1000
    for d in range(count):
        data = str(d + 1).encode()
        data_hex = hexlify(data)
        audit_path = m.append(data)
        audit_path_hex = [hexlify(h) for h in audit_path]
        incl_proof = m.inclusion_proof(d, d+1)
        assert audit_path == incl_proof
        if d % printEvery == 0:
            show(h, m, data_hex)
            print("audit path is {}".format(audit_path_hex))
            print("audit path length is {}".format(verifier.audit_path_length(
                d, d+1)))
            print("audit path calculated length is {}".format(
                len(audit_path)))
        calculated_root_hash = verifier._calculate_root_hash_from_audit_path(
            h.hash_leaf(data), d, audit_path[:], d+1)
        if d % printEvery == 0:
            print("calculated root hash is {}".format(calculated_root_hash))
        sth = STH(d+1, m.root_hash)
        verifier.verify_leaf_inclusion(data, d, audit_path, sth)

    checkConsistency(m, verifier=verifier)

    for d in range(1, count):
        verifier.verify_tree_consistency(d, d + 1,
                                         m.merkle_tree_hash(0, d),
                                         m.merkle_tree_hash(0, d + 1),
                                         m.consistency_proof(d, d + 1))

    newTree = CompactMerkleTree(hasher=h)
    m.save(newTree)
    assert m.root_hash == newTree.root_hash
    assert m.hashes == newTree.hashes

    newTree = CompactMerkleTree(hasher=h)
    newTree.load(m)
    assert m.root_hash == newTree.root_hash
    assert m.hashes == newTree.hashes

    newTree = copy(m)
    assert m.root_hash == newTree.root_hash
    assert m.hashes == newTree.hashes


def testEfficientHashStore(hasherAndTree, addTxns, storeHashes):
    h, m = hasherAndTree

    mhs = storeHashes  # type: HashStore
    txnCount, auditPaths = addTxns

    for leaf_ptr in range(1, txnCount + 1):
        print("leaf hash: {}".format(hexlify(mhs.readLeaf(leaf_ptr))))

    # make sure that there are not more leafs than we expect
    try:
        mhs.readLeaf(txnCount + 1)
        assert False
    except Exception as ex:
        assert isinstance(ex, IndexError)

    node_ptr = 0
    while True:
        node_ptr += 1
        try:
            # start, height, node_hash = mhs.readNode(node_ptr)
            node_hash = mhs.readNode(node_ptr)
        except IndexError:
            break
        print("node hash: {}".format(hexlify(node_hash)))
        # TODO: The api has changed for FileHashStore and OrientDBStore,
        # HashStore should implement methods for calculating start and
        # height of a node
        # end = start - pow(2, height) + 1
        # print("node hash start-end: {}-{}".format(start, end))
        # print("node hash height: {}".format(height))
        # print("node hash end: {}".format(end)s)
        # _, _, nhByTree = mhs.readNodeByTree(start, height)
        # assert nhByTree == node_hash


def testLocate(hasherAndTree, addTxns, storeHashes):
    h, m = hasherAndTree

    mhs = storeHashes
    txnCount, auditPaths = addTxns

    verifier = MerkleVerifier()
    startingTime = time.perf_counter()
    for d in range(50):
        print()
        pos = d+1
        print("Audit Path for Serial No: {}".format(pos))
        leafs, nodes = mhs.getPath(pos)
        calculatedAuditPath = []
        for i, leaf_pos in enumerate(leafs):
            hexLeafData = hexlify(mhs.readLeaf(leaf_pos))
            print("leaf: {}".format(hexLeafData))
            calculatedAuditPath.append(hexLeafData)
        for node_pos in nodes:
            node = mhs.readNode(node_pos)
            hexNodeData = hexlify(node)
            print("node: {}".format(hexNodeData))
            calculatedAuditPath.append(hexNodeData)
        print("{} -> leafs: {}, nodes: {}".format(pos, leafs, nodes))
        print("Audit path built using formula {}".format(calculatedAuditPath))
        print("Audit path received while appending leaf {}".format(auditPaths[d]))

        # Testing equality of audit path calculated using formula and audit path
        #  received while inserting leaf into the tree
        assert calculatedAuditPath == auditPaths[d]
        auditPathLength = verifier.audit_path_length(d, d+1)
        assert auditPathLength == len(calculatedAuditPath)

        # Testing root hash generation
        leafHash = storeHashes.readLeaf(d + 1)
        rootHashFrmCalc = hexlify(verifier._calculate_root_hash_from_audit_path(
            leafHash, d, [unhexlify(h) for h in calculatedAuditPath], d+1))
        rootHash = hexlify(verifier._calculate_root_hash_from_audit_path(
            leafHash, d, [unhexlify(h) for h in auditPaths[d]], d + 1))
        assert rootHash == rootHashFrmCalc

        print("Root hash from audit path built using formula {}".
              format(calculatedAuditPath))
        print("Root hash from audit path received while appending leaf {}".
              format(auditPaths[d]))

        print("Leaf hash length is {}".format(len(leafHash)))
        print("Root hash length is {}".format(len(rootHash)))

        # Testing verification, do not need `assert` since
        # `verify_leaf_hash_inclusion` will throw an exception
        sthFrmCalc = STH(d + 1, unhexlify(rootHashFrmCalc))
        verifier.verify_leaf_hash_inclusion(
            leafHash, d,
            [unhexlify(h) for h in calculatedAuditPath],
            sthFrmCalc)
        sth = STH(d + 1, unhexlify(rootHash))
        verifier.verify_leaf_hash_inclusion(
            leafHash, d,
            [unhexlify(h) for h in auditPaths[d]], sth)
    print(time.perf_counter()-startingTime)
