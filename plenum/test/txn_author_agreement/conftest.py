import json

import pytest
from indy.ledger import build_acceptance_mechanism_request

from common.serializers.serialization import config_state_serializer
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request

from plenum.test.helper import sdk_sign_and_submit_req_obj, sdk_get_and_check_replies

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, TXN_AUTHOR_AGREEMENT_AML, AML_VERSION, AML, AML_CONTEXT
from plenum.common.request import Request

from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory
from ledger.compact_merkle_tree import CompactMerkleTree

from plenum.common.util import randomString
from plenum.common.ledger import Ledger
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.delayers import req_delay
from plenum.test.testing_utils import FakeSomething

from plenum.test.txn_author_agreement.helper import (
    TaaData, expected_state_data, expected_data,
    TaaAmlData, expected_aml_data)


@pytest.fixture
def config_ledger(tmpdir_factory):
    tdir = tmpdir_factory.mktemp('').strpath
    return Ledger(CompactMerkleTree(),
                  dataDir=tdir)


@pytest.fixture
def config_state():
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture
def config_req_handler(config_state,
                       config_ledger):
    return ConfigReqHandler(config_ledger,
                            config_state,
                            domain_state=FakeSomething(),
                            bls_store=FakeSomething())


@pytest.fixture(scope="module")
def nodeSetWithOneNodeResponding(txnPoolNodeSet):
    # the order of nodes the client sends requests to is [Alpha, Beta, Gamma, Delta]
    # delay all requests to Beta, Gamma and Delta
    # we expect that it's sufficient for the client to get Reply from Alpha only
    # as for write requests, we can send it to 1 node only, and it will be propagated to others
    for node in txnPoolNodeSet[1:]:
        node.clientIbStasher.delay(req_delay())
    return txnPoolNodeSet


@pytest.fixture
def taa_input_data():
    return [
        TaaData(
            text=randomString(32), version=randomString(8),
            seq_no=n, txn_time=(n + 10)
        )
        for n in range(10)
    ]


@pytest.fixture
def taa_aml_input_data():
    return [
        TaaAmlData(
            version=randomString(8), aml={randomString(8): randomString(16)},
            amlContext=randomString(8)
        )
        for _ in range(10)
    ]


@pytest.fixture
def taa_expected_state_data(taa_input_data):
    return {data.version: expected_state_data(data) for data in taa_input_data}


@pytest.fixture
def taa_aml_expected_state_data(taa_aml_input_data):
    return {data.version: expected_aml_data(data) for data in taa_aml_input_data}


@pytest.fixture
def taa_expected_data(taa_input_data):
    return {data.version: expected_data(data) for data in taa_input_data}


@pytest.fixture
def taa_expected_digests(taa_input_data):
    # TODO use some other API, e.g. sdk's one
    return {data.version: ConfigReqHandler._taa_digest(data.text, data.version) for data in taa_input_data}


@pytest.fixture
def taa_aml_expected_data(taa_aml_input_data):
    # TODO use some other API, e.g. sdk's one
    return {data.version: config_state_serializer.serialize(
        {AML_VERSION: data.version, AML: data.aml, AML_CONTEXT: data.amlContext}) for data in taa_aml_input_data}


@pytest.fixture(scope="function")
def taa_aml_request(looper, sdk_wallet_trustee, sdk_pool_handle):
    return looper.loop.run_until_complete(build_acceptance_mechanism_request(
        sdk_wallet_trustee[1],
        json.dumps({
            'Nice way': 'very good way to accept agreement'}),
        randomString(), randomString()))


@pytest.fixture(scope="module")
def taa_aml_request_module(looper, sdk_wallet_trustee, sdk_pool_handle):
    return looper.loop.run_until_complete(build_acceptance_mechanism_request(
        sdk_wallet_trustee[1],
        json.dumps({
            'Nice way': 'very good way to accept agreement'}),
        randomString(), randomString()))


@pytest.fixture(scope="module")
def setup_aml(looper, txnPoolNodeSet, taa_aml_request_module, sdk_pool_handle, sdk_wallet_trustee):
    req = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, taa_aml_request_module)
    sdk_get_and_check_replies(looper, [req])
