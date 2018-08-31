import os
from argparse import ArgumentTypeError

import pytest

from common.serializers.json_serializer import JsonSerializer
from ledger.genesis_txn.genesis_txn_file_util import genesis_txn_file
from plenum.bls.bls_key_manager_file import BlsKeyManagerFile
from plenum.common.constants import NYM, VERKEY, ROLE, TARGET_NYM, ALIAS, NODE, \
    DATA, CLIENT_IP, CLIENT_PORT, NODE_IP, \
    NODE_PORT, SERVICES, BLS_KEY, VALIDATOR, TRUSTEE, STEWARD, BLS_KEY_PROOF
from plenum.common.test_network_setup import TestNetworkSetup
from plenum.common.txn_util import getTxnOrderedFields, get_seq_no, get_txn_id, get_payload_data, get_type, get_version, \
    get_protocol_version
from plenum.common.util import randomString
from storage import store_utils
from stp_zmq.zstack import ZStack

portsStart = 9600

NODE_COUNT = 4
CLIENT_COUNT = 8
TRUSTEE_COUNT = 1


@pytest.fixture()
def params(tconf):
    steward_defs, node_defs = TestNetworkSetup.gen_defs(
        ips=None, nodeCount=NODE_COUNT, starting_port=portsStart)

    client_defs = TestNetworkSetup.gen_client_defs(clientCount=CLIENT_COUNT)
    trustee_def = TestNetworkSetup.gen_trustee_def(1)
    nodeParamsFile = randomString()
    return steward_defs, node_defs, client_defs, trustee_def, nodeParamsFile


@pytest.fixture()
def bootstrap(params, tdir, tconf):
    steward_defs, node_defs, client_defs, trustee_def, nodeParamsFile = params
    TestNetworkSetup.bootstrapTestNodesCore(
        config=tconf, network="test", appendToLedgers=False,
        domainTxnFieldOrder=getTxnOrderedFields(),
        trustee_def=trustee_def, steward_defs=steward_defs,
        node_defs=node_defs, client_defs=client_defs, localNodes=1,
        nodeParamsFileName=nodeParamsFile, chroot=tdir)


@pytest.fixture()
def config_helper(config_helper_class, tdir, tconf):
    return config_helper_class(tconf, chroot=tdir)


@pytest.fixture()
def genesis_dir(config_helper):
    return config_helper.genesis_dir


@pytest.fixture()
def keys_dir(config_helper):
    return config_helper.keys_dir


@pytest.fixture()
def domain_genesis_file(genesis_dir, config_helper):
    return os.path.join(genesis_dir,
                        genesis_txn_file(TestNetworkSetup.domain_ledger_file_name(config_helper.config)))


@pytest.fixture()
def pool_genesis_file(genesis_dir, config_helper):
    return os.path.join(genesis_dir,
                        genesis_txn_file(TestNetworkSetup.pool_ledger_file_name(config_helper.config)))


def test_bootstrap_test_node_creates_genesis_files(bootstrap,
                                                   genesis_dir,
                                                   domain_genesis_file,
                                                   pool_genesis_file):
    assert os.path.exists(genesis_dir)
    assert os.path.exists(domain_genesis_file)
    assert os.path.exists(pool_genesis_file)


def test_bootstrap_test_node_creates_keys(bootstrap,
                                          keys_dir,
                                          params):
    assert os.path.exists(keys_dir)
    _, node_defs, _, _, _ = params

    # only Node1 is local, that is has keys generated
    node_name = node_defs[0].name
    node_keys_folder = os.path.join(keys_dir, node_name)
    assert os.path.exists(node_keys_folder)
    assert os.path.exists(os.path.join(node_keys_folder, ZStack.PublicKeyDirName))
    assert os.path.exists(os.path.join(node_keys_folder, ZStack.PrivateKeyDirName))
    assert os.path.exists(os.path.join(node_keys_folder, ZStack.VerifKeyDirName))
    assert os.path.exists(os.path.join(node_keys_folder, ZStack.SigKeyDirName))
    assert os.path.exists(os.path.join(node_keys_folder, BlsKeyManagerFile.BLS_KEYS_DIR_NAME))


def test_domain_genesis_txns(bootstrap, domain_genesis_file):
    serializer = JsonSerializer()
    with open(domain_genesis_file) as f:
        i = 0
        for line in store_utils.cleanLines(f.readlines()):
            txn = serializer.deserialize(line)
            assert get_seq_no(txn)
            assert get_payload_data(txn)
            assert get_type(txn) == NYM
            assert get_version(txn) == "1"
            assert get_protocol_version(txn) is None
            assert get_payload_data(txn)[VERKEY]
            assert get_payload_data(txn)[TARGET_NYM]
            assert ALIAS not in get_payload_data(txn)

            # expect Trustees, then Stewards, then Clients
            if 0 <= i < TRUSTEE_COUNT:
                expected_role = TRUSTEE
            elif TRUSTEE_COUNT <= i < TRUSTEE_COUNT + NODE_COUNT:
                expected_role = STEWARD
            else:
                expected_role = None
            assert get_payload_data(txn).get(ROLE) == expected_role
            i += 1


def test_pool_genesis_txns(bootstrap, pool_genesis_file):
    serializer = JsonSerializer()
    with open(pool_genesis_file) as f:
        for line in store_utils.cleanLines(f.readlines()):
            txn = serializer.deserialize(line)
            assert get_seq_no(txn)
            assert get_txn_id(txn)
            assert get_payload_data(txn)
            assert get_type(txn) == NODE
            assert get_version(txn) == "1"
            assert get_protocol_version(txn) is None
            assert get_payload_data(txn)[TARGET_NYM]
            data = get_payload_data(txn).get(DATA)
            assert data
            assert data[ALIAS]
            assert data[CLIENT_IP]
            assert data[CLIENT_PORT]
            assert data[NODE_IP]
            assert data[NODE_PORT]
            assert data[SERVICES] == [VALIDATOR]
            assert data[BLS_KEY]
            assert data[BLS_KEY_PROOF]


def test_check_valid_ip_host(params, tdir, tconf):
    _, _, client_defs, trustee_def, nodeParamsFile = params

    valid = [
        '34.200.79.65,52.38.24.189',
        'ec2-54-173-9-185.compute-1.amazonaws.com,ec2-52-38-24-189.compute-1.amazonaws.com',
        'ec2-54-173-9-185.compute-1.amazonaws.com,52.38.24.189,34.200.79.65',
        '52.38.24.189,ec2-54-173-9-185.compute-1.amazonaws.com,34.200.79.65',
        'ledger.net,ledger.net'
    ]

    invalid = [
        '34.200.79()3.65,52.38.24.189',
        '52.38.24.189,ec2-54-173$-9-185.compute-1.amazonaws.com,34.200.79.65',
        '52.38.24.189,ec2-54-173-9-185.com$pute-1.amazonaws.com,34.200.79.65',
        '52.38.24.189,ec2-54-173-9-185.com&pute-1.amazonaws.com,34.200.79.65',
        '52.38.24.189,ec2-54-173-9-185.com*pute-1.amazonaws.com,34.200.79.65',
    ]
    for v in valid:
        assert v.split(',') == TestNetworkSetup._bootstrap_args_type_ips_hosts(v)
        steward_defs, node_defs = TestNetworkSetup.gen_defs(
            ips=None, nodeCount=2, starting_port=portsStart)
        TestNetworkSetup.bootstrapTestNodesCore(
            config=tconf, network="test", appendToLedgers=False,
            domainTxnFieldOrder=getTxnOrderedFields(),
            trustee_def=trustee_def, steward_defs=steward_defs,
            node_defs=node_defs, client_defs=client_defs, localNodes=1,
            nodeParamsFileName=nodeParamsFile, chroot=tdir)

    for v in invalid:
        with pytest.raises(ArgumentTypeError):
            TestNetworkSetup._bootstrap_args_type_ips_hosts(v)
