import os

import pytest

from plenum.common.init_util import initialize_node_environment
from plenum.server.node import Node

SAMPLE_GEN_NODE_1 = """{"data":{"alias":"Node1","client_ip":"127.0.0.1","client_port":9701,"node_ip":"127.0.0.1","node_port":9700,"services":["VALIDATOR"]},"dest":"8WM6hggY9oqoYa8i5WxcRTHREgT1rFW1zxorh8XyKjLA","identifier":"Ssycj6ZMhXaEx5gXr6xGv2","txnId":"fea82e10e894419fe2bea7d96296a6d46f50f93f9eeda954ec461b2ed2950b62","type":"0"}"""
SAMPLE_GEN_NODE_2 = """{"data":{"alias":"Node2","client_ip":"127.0.0.1","client_port":9703,"node_ip":"127.0.0.1","node_port":9702,"services":["VALIDATOR"]},"dest":"8o28Ywvj4CvVCKub4TdyXJ9T2y6aFpyQS2iF5GUNCDvF","identifier":"K34DgPoiKHVBcaS9DeSgQv","txnId":"1ac8aece2a18ced660fef8694b61aac3af08ba875ce3026a160acbc3a3af35fc","type":"0"}"""
SAMPLE_GEN_NODE_3 = """{"data":{"alias":"Node3","client_ip":"127.0.0.1","client_port":9705,"node_ip":"127.0.0.1","node_port":9704,"services":["VALIDATOR"]},"dest":"5EAh3u5Gj8HDb7C84AxjUSNN8vxioGLsKKzUiH71RVHL","identifier":"NHD3fiLvJMQVzDuiptbAm3","txnId":"7e9f355dffa78ed24668f0e0e369fd8c224076571c51e2ea8be5f26479edebe4","type":"0"}"""
SAMPLE_GEN_NODE_4 = """{"data":{"alias":"Node4","client_ip":"127.0.0.1","client_port":9707,"node_ip":"127.0.0.1","node_port":9706,"services":["VALIDATOR"]},"dest":"6KFnarfpneosG5ez43QbLA1j3bdXke3Bu1nqUyhfTYF3","identifier":"VE7ZgKvy5tfnJxsXKXEfps","txnId":"aa5e817d7cc626170eca175822029339a444eb0ee8f0bd20d3b0b76e566fb008","type":"0"}"""

SAMPLE_GEN_4_POOL = [SAMPLE_GEN_NODE_1, SAMPLE_GEN_NODE_2,
                     SAMPLE_GEN_NODE_3, SAMPLE_GEN_NODE_3]

SAMPLE_GEN_NODE_1_DUPLICATE_KEY = """{"data":{"alias":"Node1", "alias":"Node1","client_ip":"127.0.0.1","client_port":9701,"node_ip":"127.0.0.1","node_port":9700,"services":["VALIDATOR"]},"dest":"8WM6hggY9oqoYa8i5WxcRTHREgT1rFW1zxorh8XyKjLA","identifier":"Ssycj6ZMhXaEx5gXr6xGv2","txnId":"fea82e10e894419fe2bea7d96296a6d46f50f93f9eeda954ec461b2ed2950b62","type":"0"}"""
SAMPLE_GEN_NODE_1_NULL_VALUES = """{"data":{"alias":null, "client_ip":null,"client_port":null,"node_ip":null,"node_port":null,"services":null},"dest":null,"identifier":null,"txnId":null,"type":null}"""
SAMPLE_GEN_NODE_1_COMPLEX_TARGET = """{"data":{"alias":"Node1","client_ip":"127.0.0.1","client_port":9701,"node_ip":"127.0.0.1","node_port":9700,"services":["VALIDATOR"]},"dest":{"id":"8WM6hggY9oqoYa8i5WxcRTHREgT1rFW1zxorh8XyKjLA"},"identifier":"Ssycj6ZMhXaEx5gXr6xGv2","txnId":"fea82e10e894419fe2bea7d96296a6d46f50f93f9eeda954ec461b2ed2950b62","type":"0"}"""


def _setup_genesis(base_dir, ledger_file_name, genesis_txn_list):
    default_file = os.path.join(base_dir, ledger_file_name)
    with open(default_file, 'w') as f:
        f.write("\n".join(genesis_txn_list))


@pytest.mark.skip  # INDY1-140
def test_empty_dict_in_genesis(tmpdir, looper):
    base_dir = str(tmpdir)
    name = "Node1"
    ledger_file = 'pool_transactions_sandbox'

    gen_txn = list(SAMPLE_GEN_4_POOL)
    gen_txn.insert(1, "{}")

    _setup_genesis(base_dir, ledger_file, gen_txn)

    initialize_node_environment(name=name, base_dir=base_dir)

    n = Node(name=name, basedirpath=base_dir)
    looper.add(n)


@pytest.mark.skip  # INDY1-141
def test_empty_line(tmpdir, looper):
    base_dir = str(tmpdir)
    name = "Node1"
    ledger_file = 'pool_transactions_sandbox'

    gen_txn = list(SAMPLE_GEN_4_POOL)
    gen_txn.insert(1, " ")

    _setup_genesis(base_dir, ledger_file, gen_txn)

    initialize_node_environment(name=name, base_dir=base_dir)

    n = Node(name=name, basedirpath=base_dir)
    looper.add(n)


@pytest.mark.skip
def test_utf_16(tmpdir, looper):
    base_dir = str(tmpdir)
    name = "Node1"
    ledger_file = 'pool_transactions_sandbox'

    gen_txn = list(SAMPLE_GEN_4_POOL)

    default_file = os.path.join(base_dir, ledger_file)
    genesis_data = "\n".join(gen_txn)
    with open(default_file, 'wb') as f:
        f.write(genesis_data.encode("UTF-16"))

    initialize_node_environment(name=name, base_dir=base_dir)

    n = Node(name=name, basedirpath=base_dir)
    looper.add(n)


@pytest.mark.skip
def test_utf_8_with_bom(tmpdir, looper):
    base_dir = str(tmpdir)
    name = "Node1"
    ledger_file = 'pool_transactions_sandbox'

    gen_txn = list(SAMPLE_GEN_4_POOL)

    default_file = os.path.join(base_dir, ledger_file)
    genesis_data = "\n".join(gen_txn)
    with open(default_file, 'wb') as f:
        f.write(b'\xEF\xBB\xBF')
        f.write(genesis_data.encode("UTF-8"))

    initialize_node_environment(name=name, base_dir=base_dir)

    n = Node(name=name, basedirpath=base_dir)
    looper.add(n)


@pytest.mark.skip
def test_null_values(tmpdir, looper):
    base_dir = str(tmpdir)
    name = "Node1"
    ledger_file = 'pool_transactions_sandbox'

    gen_txn = list(SAMPLE_GEN_4_POOL)
    gen_txn[0] = SAMPLE_GEN_NODE_1_NULL_VALUES

    _setup_genesis(base_dir, ledger_file, gen_txn)

    initialize_node_environment(name=name, base_dir=base_dir)

    n = Node(name=name, basedirpath=base_dir)
    looper.add(n)


@pytest.mark.skip
def test_complex_target(tmpdir, looper):
    """
        Test what happens if target is a json object instead of a String
    """
    base_dir = str(tmpdir)
    name = "Node1"
    ledger_file = 'pool_transactions_sandbox'

    gen_txn = list(SAMPLE_GEN_4_POOL)
    gen_txn[0] = SAMPLE_GEN_NODE_1_COMPLEX_TARGET

    _setup_genesis(base_dir, ledger_file, gen_txn)

    initialize_node_environment(name=name, base_dir=base_dir)

    n = Node(name=name, basedirpath=base_dir)
    looper.add(n)


@pytest.mark.skip
def test_duplicate_tnx(tmpdir, looper):
    base_dir = str(tmpdir)
    name = "Node1"
    ledger_file = 'pool_transactions_sandbox'

    gen_txn = list(SAMPLE_GEN_4_POOL)
    gen_txn[1] = SAMPLE_GEN_NODE_1

    _setup_genesis(base_dir, ledger_file, gen_txn)

    initialize_node_environment(name=name, base_dir=base_dir)

    n = Node(name=name, basedirpath=base_dir)
    looper.add(n)
