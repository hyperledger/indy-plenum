import base64
import os
import random
import string
from collections import OrderedDict

import pytest
from ledger.ledger import Ledger
from ledger.serializers.json_serializer import JsonSerializer
from ledger.serializers.compact_serializer import CompactSerializer
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.stores.file_hash_store import FileHashStore
from ledger.test.helper import NoTransactionRecoveryLedger, \
    check_ledger_generator
from ledger.util import ConsistencyVerificationFailed, F


def b64e(s):
    return base64.b64encode(s).decode("utf-8")


def b64d(s):
    return base64.b64decode(s)


def lst2str(l):
    return ",".join(l)


orderedFields = OrderedDict([
    ("identifier", (str, str)),
    ("reqId", (str, int)),
    ("op", (str, str))
])

ledgerSerializer = CompactSerializer(orderedFields)


@pytest.fixture(scope="function")
def ledger(tempdir):
    ledger = Ledger(CompactMerkleTree(hashStore=FileHashStore(dataDir=tempdir)),
                    dataDir=tempdir, serializer=ledgerSerializer)
    ledger.reset()
    return ledger


def testAddTxn(tempdir, ledger):
    txn1 = {
        'identifier': 'cli1',
        'reqId': 1,
        'op': 'do something'
    }
    ledger.add(txn1)

    txn2 = {
        'identifier': 'cli1',
        'reqId': 2,
        'op': 'do something else'
    }
    ledger.add(txn2)

    # Check that the transaction is added to the Merkle Tree
    assert ledger.size == 2

    # Check that the data is appended to the immutable store
    txn1[F.seqNo.name] = 1
    txn2[F.seqNo.name] = 2
    assert txn1 == ledger[1]
    assert txn2 == ledger[2]
    check_ledger_generator(ledger)


def testQueryMerkleInfo(tempdir, ledger):
    merkleInfo = {}
    for i in range(100):
        txn = {
            'identifier': 'cli' + str(i),
            'reqId': i+1,
            'op': ''.join([random.choice(string.printable) for i in range(
                random.randint(i+1, 100))])
        }
        mi = ledger.add(txn)
        seqNo = mi.pop(F.seqNo.name)
        assert i+1 == seqNo
        merkleInfo[seqNo] = mi

    for i in range(100):
        assert merkleInfo[i+1] == ledger.merkleInfo(i+1)


"""
If the server holding the ledger restarts, the ledger should be fully rebuilt
from persisted data. Any incoming commands should be stashed. (Does this affect
creation of Signed Tree Heads? I think I don't really understand what STHs are.)
"""


def testRecoverMerkleTreeFromLedger(tempdir):
    ledger2 = Ledger(CompactMerkleTree(), dataDir=tempdir,
                     serializer=ledgerSerializer)
    assert ledger2.tree.root_hash is not None
    ledger2.reset()
    ledger2.stop()


def testRecoverLedgerFromHashStore(tempdir):
    fhs = FileHashStore(tempdir)
    tree = CompactMerkleTree(hashStore=fhs)
    ledger = Ledger(tree=tree, dataDir=tempdir)
    for d in range(10):
        ledger.add(str(d).encode())
    updatedTree = ledger.tree
    ledger.stop()

    tree = CompactMerkleTree(hashStore=fhs)
    restartedLedger = Ledger(tree=tree, dataDir=tempdir)
    assert restartedLedger.size == ledger.size
    assert restartedLedger.root_hash == ledger.root_hash
    assert restartedLedger.tree.hashes == updatedTree.hashes
    assert restartedLedger.tree.root_hash == updatedTree.root_hash


def testRecoverLedgerNewFieldsToTxnsAdded(tempdir):
    fhs = FileHashStore(tempdir)
    tree = CompactMerkleTree(hashStore=fhs)
    ledger = Ledger(tree=tree, dataDir=tempdir, serializer=ledgerSerializer)
    for d in range(10):
        ledger.add({"identifier": "i{}".format(d), "reqId": d, "op": "operation"})
    updatedTree = ledger.tree
    ledger.stop()

    newOrderedFields = OrderedDict([
        ("identifier", (str, str)),
        ("reqId", (str, int)),
        ("op", (str, str)),
        ("newField", (str, str))
    ])
    newLedgerSerializer = CompactSerializer(newOrderedFields)

    tree = CompactMerkleTree(hashStore=fhs)
    restartedLedger = Ledger(tree=tree, dataDir=tempdir, serializer=newLedgerSerializer)
    assert restartedLedger.size == ledger.size
    assert restartedLedger.root_hash == ledger.root_hash
    assert restartedLedger.tree.hashes == updatedTree.hashes
    assert restartedLedger.tree.root_hash == updatedTree.root_hash


def testConsistencyVerificationOnStartupCase1(tempdir):
    """
    One more node was added to nodes file
    """
    fhs = FileHashStore(tempdir)
    tree = CompactMerkleTree(hashStore=fhs)
    ledger = Ledger(tree=tree, dataDir=tempdir)
    tranzNum = 10
    for d in range(tranzNum):
        ledger.add(str(d).encode())
    ledger.stop()

    # Writing one more node without adding of it to leaf and transaction logs
    badNode = (None, None, ('X' * 32))
    fhs.writeNode(badNode)

    with pytest.raises(ConsistencyVerificationFailed):
        tree = CompactMerkleTree(hashStore=fhs)
        ledger = NoTransactionRecoveryLedger(tree=tree, dataDir=tempdir)
        ledger.recoverTreeFromHashStore()
    ledger.stop()


def testConsistencyVerificationOnStartupCase2(tempdir):
    """
    One more transaction added to transactions file
    """
    fhs = FileHashStore(tempdir)
    tree = CompactMerkleTree(hashStore=fhs)
    ledger = Ledger(tree=tree, dataDir=tempdir)
    tranzNum = 10
    for d in range(tranzNum):
        ledger.add(str(d).encode())

    # Adding one more entry to transaction log without adding it to merkle tree
    badData = 'X' * 32
    value = ledger.leafSerializer.serialize(badData, toBytes=False)
    key = str(tranzNum + 1)
    ledger._transactionLog.put(key=key, value=value)

    ledger.stop()

    with pytest.raises(ConsistencyVerificationFailed):
        tree = CompactMerkleTree(hashStore=fhs)
        ledger = NoTransactionRecoveryLedger(tree=tree, dataDir=tempdir)
        ledger.recoverTreeFromHashStore()
    ledger.stop()


def testStartLedgerWithoutNewLineAppendedToLastRecord(ledger):
    txnStr = '{"data":{"alias":"Node1","client_ip":"127.0.0.1","client_port":9702,"node_ip":"127.0.0.1",' \
           '"node_port":9701,"services":["VALIDATOR"]},"dest":"Gw6pDLhcBcoQesN72qfotTgFa7cbuqZpkX3Xo6pLhPhv",' \
           '"identifier":"FYmoFw55GeQH7SRFa37dkx1d2dZ3zUF8ckg7wmL7ofN4",' \
           '"txnId":"fea82e10e894419fe2bea7d96296a6d46f50f93f9eeda954ec461b2ed2950b62","type":"0"}'
    lineSep = ledger._transactionLog.lineSep
    lineSep = lineSep if isinstance(lineSep, bytes) else lineSep.encode()
    ledger.start()
    ledger._transactionLog.put(txnStr)
    ledger._transactionLog.put(txnStr)
    ledger._transactionLog.dbFile.write(txnStr)     # here, we just added data without adding new line at the end
    size1 = ledger._transactionLog.numKeys
    assert size1 == 3
    ledger.stop()
    newLineCounts = open(ledger._transactionLog.dbPath, 'rb').read().count(lineSep) + 1
    assert newLineCounts == 3

    # now start ledger, and it should add the missing new line char at the end of the file, so
    # if next record gets written, it will be still in proper format and won't break anything.
    ledger.start()
    size2 = ledger._transactionLog.numKeys
    assert size2 == size1
    newLineCountsAferLedgerStart = open(ledger._transactionLog.dbPath, 'rb').read().count(lineSep) + 1
    assert newLineCountsAferLedgerStart == 4
    ledger._transactionLog.put(txnStr)
    assert ledger._transactionLog.numKeys == 4
