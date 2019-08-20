from common.serializers.serialization import txn_root_serializer
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.hash_stores.file_hash_store import FileHashStore
from ledger.merkle_verifier import MerkleVerifier
from ledger.util import STH


def test_compact_merkle_tree():
    data_dir = "/home/nikita/shit"
    name = "data3"
    trie = CompactMerkleTree(hashStore=FileHashStore(dataDir=data_dir,
                             fileNamePrefix=name))
    trie.append("{}".format(1).encode("utf-8"))
    trie.append("{}".format(2).encode("utf-8"))
    trie.append("{}".format(3).encode("utf-8"))
    trie.append("{}".format(4).encode("utf-8"))
    trie.append("{}".format(5).encode("utf-8"))


    verifier = MerkleVerifier()
    proof = trie.inclusion_proof(2, 5)
    root_hash = trie.merkle_tree_hash(0, 5)
    sth = STH(5, root_hash)
    # verifier.verify_leaf_inclusion("{}".format(3).encode("utf-8"), 3, proof, sth)

    print([txn_root_serializer.serialize(h) for h in proof])
    print(txn_root_serializer.serialize(root_hash))
