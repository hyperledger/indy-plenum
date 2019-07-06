import json
from copy import deepcopy
import pytest
from indy.ledger import build_acceptance_mechanisms_request

from common.serializers.serialization import config_state_serializer
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.txn_author_agreement_aml_handler import TxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.txn_author_agreement_handler import TxnAuthorAgreementHandler
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory
from ledger.compact_merkle_tree import CompactMerkleTree

from plenum.common.constants import (
    CURRENT_PROTOCOL_VERSION, TXN_AUTHOR_AGREEMENT_AML, AML_VERSION, AML, AML_CONTEXT,
    CONFIG_LEDGER_ID)
from plenum.common.request import Request
from plenum.common.ledger import Ledger
from plenum.common.util import randomString

from plenum.test.txn_author_agreement.helper import (
    TaaData, expected_state_data, expected_data,
    TaaAmlData, expected_aml_data)

from plenum.test.helper import sdk_get_and_check_replies, sdk_sign_and_submit_req_obj, get_handler_by_type_wm
from plenum.test.delayers import req_delay
from plenum.test.testing_utils import FakeSomething
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request
from .helper import (
    set_txn_author_agreement as _set_txn_author_agreement,
    get_txn_author_agreement as _get_txn_author_agreement,
    calc_taa_digest, gen_random_txn_author_agreement
)


# Note. tconf is necessary for proper config initialization
@pytest.fixture(scope="module")
def config_ledger(tconf, tmpdir_factory):
    tdir = tmpdir_factory.mktemp('').strpath
    return Ledger(CompactMerkleTree(),
                  dataDir=tdir)


@pytest.fixture(scope="module")
def config_state():
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture(scope="module")
def db_manager(config_state, config_ledger):
    db = DatabaseManager()
    db.register_new_database(CONFIG_LEDGER_ID, config_ledger, config_state)
    return db


@pytest.fixture(scope="module")
def write_manager(db_manager):
    wm = WriteRequestManager(db_manager)
    taa_handler = TxnAuthorAgreementHandler(db_manager)
    taa_aml_handler = TxnAuthorAgreementAmlHandler(db_manager)
    wm.register_req_handler(taa_handler)
    wm.register_req_handler(taa_aml_handler)
    return wm


@pytest.fixture(scope="module")
def taa_handler(write_manager):
    return get_handler_by_type_wm(write_manager, TxnAuthorAgreementHandler)


@pytest.fixture(scope="module")
def taa_aml_handler(write_manager):
    return get_handler_by_type_wm(write_manager, TxnAuthorAgreementAmlHandler)


@pytest.fixture(scope='module')
def aml_request_kwargs(sdk_wallet_trustee):
    return dict(
        identifier=sdk_wallet_trustee[1],
        reqId=5,
        protocolVersion=CURRENT_PROTOCOL_VERSION,
        operation={
            'type': TXN_AUTHOR_AGREEMENT_AML,
            AML_VERSION: randomString(),
            AML: {
                'Nice way': 'very good way to accept agreement',
                'Nicer way': 'the best way to accept agreement',
            },
            AML_CONTEXT: randomString()
        }
    )


# Note. sdk_pool_handle is necessary since it sets proper
# Protocol Version for requests
@pytest.fixture(scope="module")
def taa_aml_request_module(looper, aml_request_kwargs, sdk_pool_handle):
    res = looper.loop.run_until_complete(
        build_acceptance_mechanisms_request(
            aml_request_kwargs['identifier'],
            json.dumps(aml_request_kwargs['operation'][AML]),
            aml_request_kwargs['operation'][AML_VERSION],
            aml_request_kwargs['operation'][AML_CONTEXT]
        )
    )
    return res


@pytest.fixture(scope="function")
def taa_aml_request(looper, aml_request_kwargs, sdk_pool_handle):
    aml_request_kwargs = deepcopy(aml_request_kwargs)
    aml_request_kwargs['operation'][AML_VERSION] = randomString()
    aml_request_kwargs['operation'][AML_CONTEXT] = randomString()
    return looper.loop.run_until_complete(
        build_acceptance_mechanisms_request(
            aml_request_kwargs['identifier'],
            json.dumps(aml_request_kwargs['operation'][AML]),
            aml_request_kwargs['operation'][AML_VERSION],
            aml_request_kwargs['operation'][AML_CONTEXT]
        )
    )


# TODO serve AML routine with helpers/fixtures similar to TAA
@pytest.fixture(scope="module")
def set_txn_author_agreement_aml(
        looper, txnPoolNodeSet, taa_aml_request_module,
        sdk_pool_handle, sdk_wallet_trustee
):
    req = sdk_sign_and_send_prepared_request(
        looper, sdk_wallet_trustee, sdk_pool_handle, taa_aml_request_module)
    return sdk_get_and_check_replies(looper, [req])[0]


@pytest.fixture(scope='module')
def set_txn_author_agreement(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee
):
    def wrapped(text=None, version=None):
        random_taa = gen_random_txn_author_agreement()
        text = random_taa[0] if text is None else text
        version = random_taa[1] if version is None else version
        res = _set_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, text, version)
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
        return res

    return wrapped


@pytest.fixture(scope='module')
def get_txn_author_agreement(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client
):
    def wrapped(digest=None, version=None, timestamp=None):
        return _get_txn_author_agreement(
            looper, sdk_pool_handle, sdk_wallet_client,
            digest=digest, version=version, timestamp=timestamp
        )

    return wrapped


@pytest.fixture
def random_taa(request):
    marker = request.node.get_marker('random_taa')
    return gen_random_txn_author_agreement(
        **({} if marker is None else marker.kwargs))


@pytest.fixture
def taa_input_data():
    return [
        TaaData(*gen_random_txn_author_agreement(32, 8), n, n + 10)
        for n in range(10)
    ]


@pytest.fixture
def taa_aml_input_data():
    return [
        TaaAmlData(
            version=randomString(8), aml={randomString(8): randomString(16)},
            amlContext=randomString(8), seq_no=n, txn_time=n + 10
        )
        for n in range(10)
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
    return {data.version: calc_taa_digest(data.text, data.version) for data in taa_input_data}


@pytest.fixture
def taa_aml_expected_data(taa_aml_input_data):
    # TODO use some other API, e.g. sdk's one
    return {data.version: config_state_serializer.serialize(
        {AML_VERSION: data.version, AML: data.aml, AML_CONTEXT: data.amlContext}) for data in taa_aml_input_data}
