import base58
import pytest

from common.serializers.serialization import state_roots_serializer
from crypto.bls.bls_multi_signature import MultiSignature, MultiSignatureValue
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import get_utc_epoch

num = 0


@pytest.fixture(scope="function")
def domain_req_handler(txnPoolNodeSet):
    return txnPoolNodeSet[0].get_req_handler(DOMAIN_LEDGER_ID)


@pytest.fixture(scope="function")
def i():
    global num
    num += 1
    return num


@pytest.fixture(scope="function", params=['committed', 'not_committed'])
def is_committed(request):
    return request.param == 'committed'


@pytest.fixture(scope="function", params=['with_proof', 'no_proof'])
def with_proof(request):
    return request.param == 'with_proof'


@pytest.fixture(scope="function", params=['with_bls', 'no_bls'])
def with_bls(request):
    return request.param == 'with_bls'


def create_bls_multi_sig(encoded_root_hash):
    pool_state_root_hash = base58.b58encode(b"somefakepoolroothashsomefakepoolroothash").decode("utf-8")
    txn_root_hash = base58.b58encode(b"somefaketxnroothashsomefaketxnroothash").decode("utf-8")
    ledger_id = 1
    timestamp = get_utc_epoch()

    value = MultiSignatureValue(ledger_id=ledger_id,
                                state_root_hash=encoded_root_hash,
                                pool_state_root_hash=pool_state_root_hash,
                                txn_root_hash=txn_root_hash,
                                timestamp=timestamp)

    sign = "1q" * 16
    participants = ["q" * 32, "w" * 32, "e" * 32, "r" * 32]

    return MultiSignature(sign, participants, value)


def add_bls_multi_sig(domain_req_handler, root_hash):
    encoded_root_hash = state_roots_serializer.serialize(bytes(root_hash))
    domain_req_handler.bls_store.put(create_bls_multi_sig(encoded_root_hash))


def test_get_value_default_head_hash(domain_req_handler, with_bls, is_committed, with_proof, i):
    path = "key{}".format(i).encode()
    value = "value{}".format(i).encode()
    domain_req_handler.state.set(path, value)

    if is_committed:
        domain_req_handler.state.commit()
    if with_bls:
        add_bls_multi_sig(domain_req_handler, domain_req_handler.state.committedHeadHash)

    expected_value = value if is_committed else None
    has_proof = with_proof and with_bls
    result = domain_req_handler.get_value_from_state(path, with_proof=with_proof)

    assert expected_value == result[0]
    assert result[1] if has_proof else result[1] is None


def test_get_value_old_head_hash(domain_req_handler, is_committed, with_proof, with_bls, i):
    path1 = "111key{}".format(i).encode()
    value1 = "111value{}".format(i).encode()
    domain_req_handler.state.set(path1, value1)
    domain_req_handler.state.commit()
    state1 = domain_req_handler.state.committedHeadHash

    path2 = "222key{}".format(i).encode()
    value2 = "222value{}".format(i).encode()
    domain_req_handler.state.set(path2, value2)

    if is_committed:
        domain_req_handler.state.commit()
    if with_bls:
        add_bls_multi_sig(domain_req_handler, state1)

    expected_value = value1
    has_proof = with_proof and with_bls
    result = domain_req_handler.get_value_from_state(path1, with_proof=with_proof, head_hash=state1)

    assert expected_value == result[0]
    assert result[1] if has_proof else result[1] is None
