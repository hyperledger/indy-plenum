import base64
import itertools
from binascii import hexlify
from collections import OrderedDict

import pytest
from common.exceptions import PlenumValueError
from common.serializers.compact_serializer import CompactSerializer
from common.serializers.msgpack_serializer import MsgPackSerializer
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.test.conftest import orderedFields
from ledger.test.helper import NoTransactionRecoveryLedger, \
    check_ledger_generator, create_ledger_text_file_storage, create_default_ledger, random_txn
from ledger.test.test_file_hash_store import generateHashes
from ledger.util import ConsistencyVerificationFailed, F


def b64e(s):
    return base64.b64encode(s).decode("utf-8")


def b64d(s):
    return base64.b64decode(s)


def lst2str(l):
    return ",".join(l)


def test_add_txn(ledger, genesis_txns, genesis_txn_file):
    offset = len(genesis_txns) if genesis_txn_file else 0
    txn1 = random_txn(1)
    txn2 = random_txn(2)
    ledger.add(txn1)
    ledger.add(txn2)

    # Check that the transaction is added to the Merkle Tree
    assert ledger.size == 2 + offset

    # Check that the data is appended to the immutable store
    assert sorted(txn1.items()) == sorted(ledger[1 + offset].items())
    assert sorted(txn2.items()) == sorted(ledger[2 + offset].items())
    check_ledger_generator(ledger)


def test_stop_start(ledger, genesis_txns, genesis_txn_file):
    offset = len(genesis_txns) if genesis_txn_file else 0
    txn1 = random_txn(1)
    ledger.add(txn1)
    assert ledger.size == 1 + offset

    # stop the ledger
    ledger.stop()

    # Check that can not add new txn for stopped ledger
    txn2 = random_txn(2)
    with pytest.raises(Exception):
        ledger.add(txn2)

    # Check that the transaction is added to the Merkle Tree after re-start
    ledger.start()
    ledger.add(txn2)
    assert ledger.size == 2 + offset
    assert sorted(txn2.items()) == sorted(ledger[2 + offset].items())


def test_merkle_info_raises_error_for_invalid_seqNo(ledger):
    for seqNo in (-1, 0):
        with pytest.raises(PlenumValueError):
            ledger.merkleInfo(seqNo)


def test_query_merkle_info(ledger, genesis_txns, genesis_txn_file):
    offset = len(genesis_txns) if genesis_txn_file else 0
    merkleInfo = {}
    for i in range(100):
        mi = ledger.add(random_txn(i))
        seqNo = mi.pop(F.seqNo.name)
        assert i + 1 + offset == seqNo
        merkleInfo[seqNo] = mi

    for i in range(100):
        assert sorted(merkleInfo[i + 1 + offset].items()) == \
            sorted(ledger.merkleInfo(i + 1 + offset).items())


"""
If the server holding the ledger restarts, the ledger should be fully rebuilt
from persisted data. Any incoming commands should be stashed. (Does this affect
creation of Signed Tree Heads? I think I don't really understand what STHs are)
"""


def test_recover_merkle_tree_from_txn_log(create_ledger_callable, tempdir,
                                          txn_serializer, hash_serializer, genesis_txn_file):
    ledger = create_ledger_callable(
        txn_serializer, hash_serializer, tempdir, genesis_txn_file)
    for d in range(5):
        ledger.add(random_txn(d))
    # delete hash store, so that the only option for recovering is txn log
    ledger.tree.hashStore.reset()
    ledger.stop()

    size_before = ledger.size
    tree_root_hash_before = ledger.tree.root_hash
    tree_size_before = ledger.tree.tree_size
    root_hash_before = ledger.root_hash
    hashes_before = ledger.tree.hashes

    restartedLedger = create_ledger_callable(txn_serializer,
                                             hash_serializer, tempdir, genesis_txn_file)

    assert size_before == restartedLedger.size
    assert root_hash_before == restartedLedger.root_hash
    assert hashes_before == restartedLedger.tree.hashes
    assert tree_root_hash_before == restartedLedger.tree.root_hash
    assert tree_size_before == restartedLedger.tree.tree_size


def test_recover_merkle_tree_from_hash_store(create_ledger_callable, tempdir,
                                             txn_serializer, hash_serializer, genesis_txn_file):
    ledger = create_ledger_callable(
        txn_serializer, hash_serializer, tempdir, genesis_txn_file)
    for d in range(5):
        ledger.add(random_txn(d))
    ledger.stop()

    size_before = ledger.size
    tree_root_hash_before = ledger.tree.root_hash
    tree_size_before = ledger.tree.tree_size
    root_hash_before = ledger.root_hash
    hashes_before = ledger.tree.hashes

    restartedLedger = create_ledger_callable(txn_serializer,
                                             hash_serializer, tempdir, genesis_txn_file)

    assert size_before == restartedLedger.size
    assert root_hash_before == restartedLedger.root_hash
    assert hashes_before == restartedLedger.tree.hashes
    assert tree_root_hash_before == restartedLedger.tree.root_hash
    assert tree_size_before == restartedLedger.tree.tree_size


def test_recover_ledger_new_fields_to_txns_added(tempdir):
    ledger = create_ledger_text_file_storage(
        CompactSerializer(orderedFields), None, tempdir)
    for d in range(5):
        ledger.add(random_txn(d))
    updatedTree = ledger.tree
    ledger.stop()

    newOrderedFields = OrderedDict([
        ("identifier", (str, str)),
        ("reqId", (str, int)),
        ("op", (str, str)),
        ("newField", (str, str))
    ])
    restartedLedger = create_ledger_text_file_storage(
        CompactSerializer(newOrderedFields), None, tempdir)

    assert restartedLedger.size == ledger.size
    assert restartedLedger.root_hash == ledger.root_hash
    assert restartedLedger.tree.hashes == updatedTree.hashes
    assert restartedLedger.tree.root_hash == updatedTree.root_hash


def test_consistency_verification_on_startup_case_1(tempdir):
    """
    One more node was added to nodes file
    """
    ledger = create_default_ledger(tempdir)
    tranzNum = 10
    for d in range(tranzNum):
        ledger.add(random_txn(d))
    # Writing one more node without adding of it to leaf and transaction logs
    badNode = (None, None, ('X' * 32))
    ledger.tree.hashStore.writeNode(badNode)
    ledger.stop()

    with pytest.raises(ConsistencyVerificationFailed):
        tree = CompactMerkleTree(hashStore=ledger.tree.hashStore)
        ledger = NoTransactionRecoveryLedger(tree=tree, dataDir=tempdir)
        ledger.recoverTreeFromHashStore()
    ledger.stop()


def test_consistency_verification_on_startup_case_2(tempdir):
    """
    One more transaction added to transactions file
    """
    ledger = create_default_ledger(tempdir)
    tranzNum = 10
    for d in range(tranzNum):
        ledger.add(random_txn(d))

    # Adding one more entry to transaction log without adding it to merkle tree
    badData = random_txn(50)
    value = ledger.serialize_for_txn_log(badData)
    key = str(tranzNum + 1)
    ledger._transactionLog.put(key=key, value=value)

    ledger.stop()

    with pytest.raises(ConsistencyVerificationFailed):
        tree = CompactMerkleTree(hashStore=ledger.tree.hashStore)
        ledger = NoTransactionRecoveryLedger(tree=tree, dataDir=tempdir)
        ledger.recoverTreeFromHashStore()
    ledger.stop()


def test_recover_merkle_tree_from_txn_log_if_hash_store_runs_ahead(create_ledger_callable, tempdir,
                                                                   txn_serializer, hash_serializer,
                                                                   genesis_txn_file):
    '''
    Check that tree can be recovered from txn log if recovering from hash store failed
    (we have one more txn in hash store than in txn log, so consistency verification fails).
    '''
    ledger = create_ledger_callable(
        txn_serializer, hash_serializer, tempdir, genesis_txn_file)
    for d in range(5):
        ledger.add(random_txn(d))

    size_before = ledger.size
    tree_root_hash_before = ledger.tree.root_hash
    tree_size_before = ledger.tree.tree_size
    root_hash_before = ledger.root_hash
    hashes_before = ledger.tree.hashes

    # add to hash store only
    ledger._addToTree(ledger.serialize_for_tree(random_txn(50)),
                      serialized=True)
    ledger.stop()

    restartedLedger = create_ledger_callable(txn_serializer,
                                             hash_serializer, tempdir, genesis_txn_file)

    assert size_before == restartedLedger.size
    assert root_hash_before == restartedLedger.root_hash
    assert hashes_before == restartedLedger.tree.hashes
    assert tree_root_hash_before == restartedLedger.tree.root_hash
    assert tree_size_before == restartedLedger.tree.tree_size


def test_recover_merkle_tree_from_txn_log_if_hash_store_lags_behind(create_ledger_callable, tempdir,
                                                                    txn_serializer, hash_serializer,
                                                                    genesis_txn_file):
    '''
    Check that tree can be recovered from txn log if recovering from hash store failed
    (we have one more txn in txn log than in hash store, so consistency verification fails).
    '''
    ledger = create_ledger_callable(
        txn_serializer, hash_serializer, tempdir, genesis_txn_file)
    for d in range(100):
        ledger.add(random_txn(d))

    # add to txn log only
    ledger._addToStore(ledger.serialize_for_txn_log(random_txn(50)),
                       serialized=True)
    ledger.stop()

    size_before = ledger.size
    tree_size_before = ledger.tree.tree_size

    restartedLedger = create_ledger_callable(txn_serializer,
                                             hash_serializer, tempdir, genesis_txn_file)

    # root hashes will be not the same as before (since we recoverd based on txn log)
    # the new size is 1 greater than before since we recovered from txn log which contained one more txn
    assert size_before + 1 == restartedLedger.size
    assert tree_size_before + 1 == restartedLedger.tree.tree_size


def test_start_ledger_without_new_line_appended_to_last_record(tempdir, txn_serializer):
    if isinstance(txn_serializer, MsgPackSerializer):
        # MsgPack is a binary one, not compatible with TextFileStorage
        return

    ledger = create_ledger_text_file_storage(txn_serializer, None, tempdir)

    txnStr = '{"data":{"alias":"Node1","client_ip":"127.0.0.1","client_port":9702,"node_ip":"127.0.0.1",' \
        '"node_port":9701,"services":["VALIDATOR"]},"dest":"Gw6pDLhcBcoQesN72qfotTgFa7cbuqZpkX3Xo6pLhPhv",' \
        '"identifier":"FYmoFw55GeQH7SRFa37dkx1d2dZ3zUF8ckg7wmL7ofN4",' \
        '"txnId":"fea82e10e894419fe2bea7d96296a6d46f50f93f9eeda954ec461b2ed2950b62","type":"0"}'
    lineSep = ledger._transactionLog.lineSep
    lineSep = lineSep if isinstance(lineSep, bytes) else lineSep.encode()
    ledger.start()
    ledger._transactionLog.put(None, txnStr)
    ledger._transactionLog.put(None, txnStr)
    ledger._transactionLog.db_file.write(txnStr)
    # here, we just added data without adding new line at the end
    size1 = ledger._transactionLog.size
    assert size1 == 3
    ledger.stop()
    newLineCounts = open(ledger._transactionLog.db_path, 'rb')\
        .read().count(lineSep) + 1
    assert newLineCounts == 3

    # now start ledger, and it should add the missing new line char at the end of the file, so
    # if next record gets written, it will be still in proper format and won't
    # break anything.
    ledger.start()
    size2 = ledger._transactionLog.size
    assert size2 == size1
    newLineCountsAferLedgerStart = open(
        ledger._transactionLog.db_path, 'rb').read().count(lineSep) + 1
    assert newLineCountsAferLedgerStart == 4
    ledger._transactionLog.put(None, txnStr)
    assert ledger._transactionLog.size == 4


def test_add_get_txns(ledger_no_genesis):
    ledger = ledger_no_genesis
    txns = []
    hashes = [hexlify(h).decode() for h in generateHashes(40)]
    for i in range(20):
        txns.append({
            'identifier': hashes.pop(),
            'reqId': i,
            'op': hashes.pop()
        })

    for txn in txns:
        ledger.add(txn)

    check_ledger_generator(ledger)

    for frm, to in [(1, 20), (3, 8), (5, 17), (6, 10), (3, 3),
                    (3, None), (None, 10), (None, None)]:
        for s, t in ledger.getAllTxn(frm=frm, to=to):
            assert txns[s - 1] == t

    # with pytest.raises(AssertionError):
    #     list(ledger.getAllTxn(frm=3, to=1))

    for frm, to in [(i, j) for i, j in itertools.permutations(range(1, 21),
                                                              2) if i <= j]:
        for s, t in ledger.getAllTxn(frm=frm, to=to):
            assert txns[s - 1] == t
